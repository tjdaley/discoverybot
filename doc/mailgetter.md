# mailgetter.py - Retrieves PDFs from an email inbox

## Imports

This program imports a few modules to help it do its work.

First is the Python standard email module, ```email```. It takes a raw email message
and converts it into an object that is much easier to work with.

```python
import email
```

Next, is a module that we will use to help us name files. When an email client attaches
a file to an email message, part of the attachment should include the original file name,
but that is not a requirement. Therefore, we might get attachments that are PDFS (or anything else),
but without a filename to look at, we have to look to the MIME type to guess a file extension
(e.g. PDF, PNG, JPG, TXT, etc.). The ```mimetypes``` modules simplifies the task of guessing
a filename extension based on the MIME type.

```python
import mimetypes
```

That's it for the standard Python modules. Next, we import custom modules. The most important
one is the ```imapclient``` module. It allows us to connect to an email server, login, retrieve
email messages, and mark them as having been processed.

```python
from imapclient import IMAPClient, exceptions as IMAPExceptions
```

The ```logger``` module defines a simple class that is a wrapper to Python's standard *logging* module.
You'll see this used instead of ```print()``` statements. It allows us to control what messages get logged
to the screen and to a log file.

```python
from util.logger import Logger
```

The ```database``` module defines a class that wraps our functions for CRUD operations on our persistent
datastore. For now, it's implemented on top of MongoDb, but the API exposed by this class is intended to
be implementation-agnostic and thus amenable to changes in plans.

```python
from util.database import Database
```

# The MailGetter Class

The **MailGetter** class contains all the functions needed to connect to a mail server, login using our
username and password, check for new email messages, process them, save PDFs, and mark the email as
having been seen so that we don't process it again.

The first thing we do, as in any class definition, is define an initialization method. Here, the
initialization method doesn't do much: It creates a logger, loads run-time parameters, and then
creates a placeholder for the email server object that we'll create later.

```python
class MailGetter(object):
    """
    Encapsulates behavior of retrieving email, saving PDFs, and moving emails.
    """
    
    def __init__(self):
        """
        Class initializer.
        """
        self.logger = Logger.get_logger()
        self.server = None
```

## The connect() Method

Once we've loaded our run-time parameters (which provides us with a server, username, and password),
the next thing we have to do is connect to the server and login. The *connect()* method takes care of that.
You'll see that most of the *connect()* method's code relates to catching, logging, and reporting errors.

```python
    def connect(self)->bool:
        """
        Connect and login to the remote IMAP server.

        Returns:
            (bool): True if successful, otherwise False
        """
        mailserver = os.environ.get('mailserver')
        mailport = os.environ.get('mailport')
        mailssl = os.environ.get('mailssl)
        username = os.environ.get('mail_username')
        password = os.environ.get('mail_password')

        try:
            self.server = IMAPClient(mailserver, mailport, mailssl, use_uid=True)
            self.server.login(username, password)
        except ConnectionRefusedError as e:
            self.logger.fatal("Connection to %s:%s was refused: %s", mailserver, mailport, e)
            return False
        except Exception as e:
            self.logger.fatal("Error connecting to %s:%s: %s", mailserver, mailport, e)
            return False

        return True
```

## The check_folders() Method

Before we get too involved with processing email, we first want to make sure we have all the folders
that our application needs. This application only needs an INBOX folder at this time. The INBOX folder is where
new email messages will appear. In a *future* version, after we process an email message, we might move it to
a PROCESSED folder. We're not doing that right now, but the *check_folders()* method makes sure it exists
anyway.

The *check_folders()* method does three things:

1. It makes sure we have an INBOX folder. The exact name of the INBOX folder is specified in our
run-time parameters (.env). If the INBOX folder does not exist, we have a serious problem
and will not be able to continue.
2. Next, it checks for the PROCESSED folder. Again, the exact name of that folder is specified in our
run-time parameters file. If the PROCESSED folder does not exist, then we try to create it. If it does not
exist and we cannot create it, again we have a serious problem and will not be able to continue.
3. If the INBOX exists and the PROCESSED folder is there, either because it was already there or we were
able to create it, this method will return a **True** value indicating that the program can continue to the
next step. Otherwise, if we can't continue, it will return **False**.

```python
    def check_folders(self)->bool:
        """
        Check to see if we have the required INBOX and PROCESSED folders.
        If we do not have an inbox, we can't go on.
        If we do not have a processed folder, try to create it.

        Returns:
            (bool): True if successful, otherwise false.
        """

        # First, make sure the INBOX folder exists. If it does not exist, we have a serious problem
        # and need to quit.
        inbox = os.environ.get('inbox')
        if not self.server.folder_exists(inbox):
            self.logger.fatal("Error locating INBOX named '%s'.", inbox)
            return False

        # Next, see if the PROCESSED folder exists. If it does not, try to create it.
        # If we try to create it and the creation fails, again, we have a serious problem and cannot
        # continue.
        processed_folder = os.environ.get('processed_folder')
        if not self.server.folder_exists(processed_folder):
            self.logger.error("Error locating PROCESSED folder named '%s'.", processed_folder)
            self.logger.error("Will attempt to create folder named '%s'.", processed_folder)

            try:
                message = self.server.create_folder(processed_folder)
                self.logger.info("Successfully created '%s': %s", processed_folder, message)
            except Exception:
                self.logger.fatal("Failed to create '%s': %s", processed_folder, message)
                return False

        self.logger.info("Folder check was successful.")
        return True
```

## The process_inbox() Method

After we've determined that our INBOX and PROCESSED folders exist on the email server, it's time to
process the messages in our INBOX.

1. Select the INBOX folder so that subsequent operations know to use that folder.
2. Search the INBOX folder for messages that we have never seen before. When a new message arrives, it
will NOT have the *Seen* flag set, thus it is "unseen" and will be selected for processing. See below
how we change those flags once we try to process a message.
3. Once we have a list of UNSEEN messages, process them in a *for loop*. Within the *for loop*, we will
extract the email message data and hand it to that *email* module discussed above. That module will take
the raw bytes we received from the email server and convert it into an *email* object instance that is
much easier for us to work with. You'll see that in the *process_message()* section, further below.
4. After the bytes are converted to an email message instance, we pass that email message to the
*process_message()* method. The *process_message()* method will return a value of **True** if it was successful
in processing the message or **False** if it was not.
5. If we get a **True** result from *process_message()*, then mark this message as SEEN. If we get a **False**
result from *process_message()*, then mark this message as SEEN and FLAGGED FOR FOLLOWUP.

```python
    def process_inbox(self):
        """
        Process each message in the INBOX. After a message is processed:

            If processing was successful: Mark as "SEEN" so that we don't process it again.

            If processing was not successful: Mark as "SEEN" and "FOLLOWUP" so that an operator can fix
            if and requeue it by clearing the SEEN flag.
        """
        inbox = os.environ.get('inbox')
        select_info = self.server.select_folder(inbox)
        self.logger.debug("%d messages in %s.", select_info[b'EXISTS'], inbox)

        messages = self.server.search(criteria='UNSEEN')

        for msgid, data in self.server.fetch(messages, ['RFC822']).items():
            email_message = email.message_from_bytes(data[b'RFC822'])

            if self.process_message(msgid, email_message):
                # Mark message as "Seen". For now, we *won't* move the message to the PROCESSED folder.
                self.server.set_flags(msgid, b'\\Seen')
            else:
                # If we had an error processing the message, mark it for follow AND as seen.
                self.server.set_flags(msgid, [b'\\Flagged for Followup', b'\\Seen'])
```

## The process_message() Method

The *process_message()* method handles the details of processing each individual email message.

This method handles three scenarios:

1. **ATTACHED PDF**: If a PDF is attached to the message, the file is saved with a unique name.
2. **HTML LINK**: If the PDF is referenced by an HTML *anchor tag* ("a") that has an *href* property, the file is retrieved and saved with a unique name.
3. **TEXT LINK**: If the PDF is referenced by a text string, e.g., ```https://www.site.com/file.pdf```, the file is retrieved and saved with a unique name, e.g. *msgid*-file.pdf in this case.

What is NOT handled?

1. **Incomplete URLs**. If the URL of a link does not contain an "htp* or "https" prefex, we won't catch it.
2. **Indirect Linkes**. If the URL redirects us to the PDF, such as might happen if someone gets a DropBox link or a tinyurl, we
don't handled the redirection.

One thing you'll notice is that we put the *msgid* at the beginning of all the filenames. The main reason for doing that
is to prevent some freak from sending us a file name that is designed to clobber one of the files that belongs to
our system. For example, he could name it "/../mailgetter.py" and that would overright our program code and break the
system. Prepending the *msgid* to the filename protects us from that because, worst case, it creates an invalid filename
which we are unable to save. If someone is sending us nefarious junk, we don't want to save it.

You'll notice a *sanitize_from_name* function being called. It's described later, but all it does it strips the leading
and trailing angle brackets that sometimes appear around email addresses, e.g. < tom@powerdaley.com >.

If *process_message()* is able to sucessfully handle the message, it returns a **True** value. Otherwise, it returns
a **False** value. You should recall from above that this **True** or **False** value is provided to the
*process_inbox()* method and is used to determine how the email message is marked on the email server.

```python
    def process_message(self, msgid, message)->bool:
        """
        Process one message.

        Args:
            msgid (str): Unique ID for this message.
            message (email): Email message to process.
        Returns:
            (bool): True if successful, otherwise False
        """
        self.logger.debug("ID #%s: From: %s; Subject: %s", msgid, message.get("From"), message.get("Subject"))

        # Go through each part of the message. If we find a pdf file, save it.
        counter = 1 # number of attachments we've processed so far for this message.
        for part in message.walk():
            try:
                # multipart/* are just containers...skip them
                if part.get_content_maintype() == "multipart":
                    continue

                # Extract and sanitize the filename. Create a filename if one is not given.
                filename = part.get_filename()
                extension = mimetypes.guess_extension(part.get_content_type()) or ".bin"
                if not filename:
                    sanitized_from_name = sanitize_from_name(message.get("From"))
                    filename = "{}-{}-part-{}{}".format(msgid, sanitized_from_name, counter, extension)
                else:
                    filename = "{}-{}".format(msgid, filename)

                counter += 1

                # Save the attached file
                # For now, only save files of type ".PDF"
                # TODO: Parse HTML parts to see if we have links to PDF files stored elsewhere.
                upper_extension = extension.upper()

                # Save attached file . . .
                if upper_extension == ".PDF":
                    with open("{}/{}".format(os.environ.get('input_path'), filename), "wb") as fp:
                        fp.write(part.get_payload(decode=True))

                # Save file referenced by a link . . .
                elif upper_extension == ".HTML" or upper_extension == ".HTM":
                    links = extract_html_links(part.get_payload())
                    self.save_linked_files(links, msgid)
                elif upper_extension == ".BAT":
                    links = extract_text_links(part.get_payload())
                    self.save_linked_files(links, msgid)
                else:
                    self.logger.info("Skipping: (%s) %s", filename, part.get_payload(decode=True))
            except Exception as e:
                self.logger.error("Error processing attachment #%s from message #%s from %s: %s",
                    counter, msgid, message.get("From"), e)
                self.logger.exception(e)
                return False
            
        return True
```

## The sanitize_from_name() Method

As describe above, this is a simple method that transforms the *from* address contained on an email
message into something that can be used as part of a filename. For now, it just strips the < and > from
around the email address we retrieve from the email message.

```python
def sanitize_from_name(from_name:str)->str:
    """
    Sanitize the "from" name (email address of sender) for use as part of
    a file name.

    Args:
        from_name (str): "From" property of email message.
    Returns:
        (str): Sanitized version of name suitable for use as part of a filename.
    """
    result = from_name + ""

    # Strip any "<" from the beginning of the name
    if result[:1] == "<":
        result = result[1:]

    # Strip and ">" from the end of the name
    if result[-1:] == ">":
        result = result[:-1]

    return result
```

## The extract_text_links() Method

This function extracts links just pasted in as plain text. If someone rights clicks on a link,
selects the "Copy Link" option, and then pastes the result into a plain-text email message,
this is where it will be handled.

```python
TEXT_LINK_REGEX = re.compile(r'(http|https)://(.*?\.pdf)', re.IGNORECASE)
def extract_text_links(text_content:str)->list:
    """
    Extract a plain-text link passed to us. Email text content will probably be in quote-printable
    form (e.g. 3dutf-8). Therefore, the first step is to convert from quote-printable
    to a regular UTF-8 string. Next, search this string for links.

    From: https://www.mschweighauser.com/fast-url-parsing-with-python/

    Args:
        text_content (str): HTML content to parse.

    Returns:
        (list): List of links. List is empty is no links are found.
    """
    my_content = quopri.decodestring(text_content).decode('utf-8')
    return ["{}://{}".format(match[0],match[1]) for match in TEXT_LINK_REGEX.findall(my_content)]
```

## The extract_html_links() Method

This function extracts links from an HTML-formatted part of the mail message.
If someone forwards us an email from the e-file system or pastes a link into an HTML-formatted message,
it will be handled here. TODO: This method could probably be eliminated and the ```extract_text_links()``` method
used instead. I write this function first, then expexted to have to do something quite different for plain
text messages. However, the ```TEXT_LINK_REGEX``` is probably sufficiently robust to handle both cases.

```python
HTML_TAG_REGEX = re.compile(r'<a[^<>]+?href=([\'\"])(.*?)\1', re.IGNORECASE)
def extract_html_links(html_content:str)->list:
    """
    Extract the value of the HREF property from any links ("a" tags) found in the
    html_content passed to us. Email text content will probably be in quote-printable
    form (e.g. 3dutf-8). Therefore, the first step is to convert from quote-printable
    to a regular UTF-8 string. Next, search this string for links.

    From: https://www.mschweighauser.com/fast-url-parsing-with-python/

    Args:
        html_content (str): HTML content to parse.

    Returns:
        (list): List of links. List is empty is no links are found.
    """
    my_content = quopri.decodestring(html_content).decode('utf-8')
    return [match[1] for match in HTML_TAG_REGEX.findall(my_content)]
```

## Final bit of code.

When this program starts, the ```if __name__ == "__main__"``` line is executed. If this program was
invoked from the command line, e.g.

```
$ python mailgetter.py
```

Then the built-in variable ```__name__``` will be equal to the string  ```__main__```. In that case
it will call the *main()* method which pulls all the rest of this code together:

1. Creates an instance of *MailGetter*.
2. Tries to connect and login to the email server.
3. If the connect/login operations are successful, then it checks to see if we have the INBOX and PROCESSED
folders that we need.
4. If we have the folders that we need, it processes the INBOX and, when it's done, the program ends.

```python
def main():
    mailgetter = MailGetter()
    if mailgetter.connect():
        if mailgetter.check_folders():
            mailgetter.process_inbox()

if __name__ == "__main__":
    main()
```

### The Author
Thomas J. Daley is a family law attorney. He handles all family law matters including divorce, custody,
child support, modification, enforcement, adoption, and termination. You can learn more about his
law practice at https://www.powerdaley.com.