"""
mailgetter.py - Retrieve email messages addressed to this service.

Copyright (c) 2019 by Thomas J. Daley, J.D. All Rights Reserved.
"""
import email
import imaplib
import mimetypes
import re
import requests
import os
import quopri
import urllib.parse

from imapclient import IMAPClient

from util.botqueue import BotQueue
from util.logger import Logger
from util.serverlock import ServerLock
import util.env  # noqa

# Uses this port to make sure only one version of this program
# is running on this server. Would like a distributed locking mechanism
# but I can't afford one right now. ;-)
LOCK_PORT = int(os.environ.get('LOCK_PORT', 9000))

MAIN = "MAILGETTER"


class MailGetter(object):
    """
    Encapsulates behavior of retrieving email, saving PDFs, and moving emails.
    """
    def __init__(self):
        """
        Class initializer.
        """
        mimetypes.init()
        logger = Logger()
        self.logger = logger.get_logger(MAIN + ".Cls")
        self.server = None
        self.queue = BotQueue()
        if not _check_paths(['input_path']):
            self.logger.fatal("File paths could not be created...cannot continue.")
            exit()

    def connect(self) -> bool:
        """
        Connect and login to the remote IMAP server.

        Returns:
            (bool): True if successful, otherwise False
        """
        mailserver = os.environ.get('mailserver')
        imapport = os.environ.get('imapport')
        mailssl = os.environ.get('mailssl')
        try:
            self.server = IMAPClient(
                mailserver,
                port=imapport,
                ssl=mailssl,
                use_uid=True
            )
            username = os.environ.get('mail_username')
            password = os.environ.get('mail_password')
            self.logger.debug(f"Username: {username}, Password: {password}")
            self.server.login(username, password)
        except ConnectionRefusedError as e:
            self.logger.fatal(f"Connection to {mailserver}:{imapport} was refused: {str(e)}")
            return False
        except Exception as e:
            self.logger.fatal(f"Connection to {mailserver}:{imapport} was refused: {str(e)}")
            return False

        return True

    def reconnect(self) -> bool:
        """
        Reconnect to the email server.

        Returns:
            (bool): True if successful, otherwise False
        """
        self.logger.debug("Reconnecting to server.")

        try:
            self.server.idle_done()
        except Exception as e:
            self.logger.error("Error exiting IDLE mode: %s", e)
            self.logger.info("Will try to reconnect anyway.")

        try:
            self.server.logout()
        except Exception as e:
            self.logger.error("Error disconnecting from IMAP server: %s", e)
            self.logger.info("Will try to reconnect anyway.")

        return self.connect()

    def check_folders(self) -> bool:
        """
        Check to see if we have the required INBOX and PROCESSED folders.
        If we do not have an inbox, we can't go on.
        If we do not have a processed folder, try to create it.

        Returns:
            (bool): True if successful, otherwise false.
        """

        # First, make sure the INBOX folder exists. If it does not exist,
        # we have a serious problem and need to quit.
        inbox = os.environ.get('inbox')
        if not self.server.folder_exists(inbox):
            self.logger.fatal(f"Error locating INBOX named '{inbox}'.")
            return False

        # Next, see if the PROCESSED folder exists. If it does not, try to
        # create it. If we try to create it and the creation fails, again,
        # we have a serious problem and cannot continue.
        processed_folder = os.environ.get('processed_folder')
        if not self.server.folder_exists(processed_folder):
            self.logger.error(f"Error locating PROCESSED folder named '{processed_folder}'.")
            self.logger.error(f"Will attempt to create folder named '{processed_folder}'.")

            try:
                message = self.server.create_folder(processed_folder)
                self.logger.info(
                    "Successfully created '%s': %s",
                    processed_folder,
                    message
                )
            except Exception:
                self.logger.fatal(
                    "Failed to create '%s': %s",
                    processed_folder,
                    message
                )
                return False

        self.logger.info("Folder check was successful.")
        return True

    def save_linked_files(
        self,
        links: list,
        msgid: str,
        from_email: str,
        subject: str,
        reply_to: str
    ):
        """
        Download and save files referenced by a link instead of directly
        attached to the email message. Raises exceptions rather than catches
        them so that the caller's error handler can deal with and record
        the error. Setting *allow_redirects* to ```True``` let's us retrieve
        files from cloud services such as DropBox and from URL smashers like
        www.tinyurl.com.

        Args:
            links (list): List of links to download from.
            msgid (str): ID of the email message we are processing. Used for
                         filename disambiguation.
            from_email (str): Apparent sender of the email.
            subject (str): Subject line of the email.
            reply_to (str): Reply-To Address of the email
        """
        input_path = os.environ.get('input_path')
        for link in links:
            if link[-4:].upper() == ".PDF":
                my_link = cloudize_link(link)
                self.logger.debug("Found link: %s", my_link)
                content = requests.get(my_link, allow_redirects=True).content
                filename = "{}/{}-{}".format(
                    input_path,
                    msgid,
                    urllib.parse.unquote(link[link.rfind("/")+1:])
                )
                with open(filename, "wb") as fp:
                    fp.write(content)
                self.queue.publish(arrival_notification(
                    from_email, reply_to, subject, filename, 'application/pdf')
                )

    def process_message(self, msgid, message) -> bool:
        """
        Process one message.

        Args:
            msgid (str): Unique ID for this message.
            message (email): Email message to process.
        Returns:
            (bool): True if successful, otherwise False
        """
        self.logger.debug(
            "ID #%s: From: %s; Subject: %s",
            msgid,
            message.get("From"),
            message.get("Subject")
        )
        from_email = sanitize_from_name(message.get("From"))
        reply_to = sanitize_from_name(message.get("Return-Path") or from_email)
        input_path = os.environ.get('input_path')

        try:
            subject = message.get("Subject")
        except OSError as e:
            self.logger.error("Cannot extract message subject: %s", str(e))
            subject = "N/A"

        for key in message.keys():
            self.logger.debug("%s = %s", key, message.get(key, None))

        # Go through each part of the message. If we find a pdf file, save it.
        counter = 1  # number of attachments we've processed for this message.
        for part in message.walk():
            try:
                # multipart/* are just containers...skip them
                if part.get_content_maintype() == "multipart":
                    continue

                # Extract & sanitize filename. Create a filename if not given.
                filename = part.get_filename()
                self.logger.debug("*** %s ***", filename)
                extension = mimetypes.guess_extension(part.get_content_type())\
                    or ".bin"
                if not filename:
                    filename = "{}-{}-part-{}{}".format(
                        msgid,
                        from_email,
                        counter,
                        extension
                    )
                else:
                    filename = "{}-{}"\
                        .format(msgid, filename)\
                        .replace("\r", "")\
                        .replace("\n", "")

                counter += 1

                # Save the attached file
                # For now, only save files of type ".PDF"
                # TODO: Parse HTML parts to see if we have links to PDF files
                # stored elsewhere.
                lower_extension = extension.lower()

                # Save attached file . . .
                if lower_extension in ['.pdf', '.docx', '.doc', '.rtf']:
                    try:
                        mimetype = mimetypes.types_map[lower_extension]
                    except KeyError as error:
                        self.logger.error(
                            "Cannot map '%s' to a mime type: %s",
                            lower_extension,
                            str(error)
                        )
                        mimetype = None

                    if mimetype is not None:
                        filename = "{}/{}".format(
                            input_path,
                            filename
                        )
                        with open(filename, "wb") as fp:
                            fp.write(part.get_payload(decode=True))
                        self.queue.publish(arrival_notification(
                            from_email,
                            reply_to,
                            subject,
                            filename,
                            mimetype)
                        )

                # Save file referenced by a link . . .
                elif lower_extension[:4] == ".htm":
                    links = extract_html_links(part.get_payload())
                    self.save_linked_files(
                        links,
                        msgid,
                        from_email,
                        subject,
                        reply_to
                    )
                elif lower_extension == ".bat":
                    links = extract_text_links(part.get_payload())
                    self.save_linked_files(
                        links,
                        msgid,
                        from_email,
                        subject,
                        reply_to
                    )
                else:
                    self.logger.info("Skipping: %s", filename)
            except Exception as e:
                self.logger.error(
                    "Error with attachment #%s from message #%s from %s: %s",
                    counter, msgid, message.get("From"), e
                )
                self.logger.exception(e)
                return False

        return True

    def wait_for_messages(self):

        # FIRST: Process anything that's already in our INBOX
        self.process_inbox()

        # NEXT: Go into IDLE mode waiting for either a timeout or new mail.
        self.server.idle()
        stay_alive = True

        # Number of times we've returned from idle without receiving any
        # messages.
        idle_counter = 0

        # Number of seconds to wait for more messages before timing out.
        idle_timeout = 60

        # If we don't receive something within this many seconds, we'll
        # reconnect to the server.
        reconnect_seconds = 300

        while stay_alive:
            try:
                # Wait for up to *idle_timeout* seconds for new messages.
                responses = self.server.idle_check(timeout=idle_timeout)
                self.logger.debug("Response to idle_check(): %s", responses)

                if responses:
                    # We DID get new messages. Process them.
                    idle_counter = 0
                    responses = self.server.idle_done()
                    self.logger.debug("Response to idle_done(): %s", responses)
                    self.process_inbox()
                    self.server.idle()
                else:
                    # We did not get any new messages.
                    idle_counter += 1

                    # If we've run out of patience, reconnect and resume idle
                    # mode.
                    if idle_counter * idle_timeout > reconnect_seconds:
                        if self.reconnect():
                            idle_counter = 0
                            self.process_inbox()
                            self.server.idle()

                        # Reconnect failure (!)
                        else:
                            stay_alive = False

            except imaplib.IMAP4.abort as e:
                self.logger.error("IMAP connection closed by host: %s", e)
                self.logger.error("Will try to reconnect")
                if self.reconnect():
                    idle_counter = 0
                    self.process_inbox()
                    self.server.idle()
                else:
                    self.logger.error("Unable to reconnect. Shutting down.")
                    stay_alive = False
            except Exception as e:
                self.logger.error("Error in IDLE loop: %s", e)
                self.logger.exception(e)
                self.logger.error("Shutting down due to the above errors.")
                stay_alive = False

    def process_inbox(self):
        """
        Process each message in the INBOX. After a message is processed:

            If processing was successful: Mark as "SEEN" so that we don't
            process it again.

            If processing was not successful: Mark as "SEEN" and "FOLLOWUP" so
            that an operator can fix if and requeue it by clearing the SEEN
            flag.
        """
        select_info = self.server.select_folder(os.environ.get('inbox'))
        self.logger.debug(
            "%d messages in %s.",
            select_info[b'EXISTS'],
            os.environ.get('inbox')
        )

        messages = self.server.search(criteria='UNSEEN')

        for msgid, data in self.server.fetch(messages, ['RFC822']).items():
            email_message = email.message_from_bytes(data[b'RFC822'])

            if self.process_message(msgid, email_message):
                # Mark message as "Seen". For now, we *won't* move the message
                # to the PROCESSED folder.
                self.server.set_flags(msgid, b'\\Seen')
            else:
                # If we had an error processing the message, mark it for
                # follow AND as seen.
                self.server.set_flags(
                    msgid,
                    [b'\\Flagged for Followup', b'\\Seen']
                )


def arrival_notification(
    email_from: str,
    reply_to: str,
    subject: str,
    filename: str,
    mime_type: str
) -> dict:
    """
    Create an arrival notification message to be published to our outbound
    queue.

    Args:
        email_from (str): Email we received the message from.
        reply_to (str): Email we should reply to.
        subject (str): Subject line from email.
        filename (str): Name of file we have extracted.
        mime_type (str): MIME-type for this file.

    Returns:
        (dict): Item to publish to outbound queue.
    """
    return {
        'publisher': "mailgetter",
        'email_from': email_from,
        'reply_to': reply_to,
        'subject': subject,
        'filename': filename,
        'mime_type': mime_type
    }


def sanitize_from_name(from_name: str) -> str:
    """
    Sanitize the "from" name (email address of sender) for use as part of
    a file name.

    Args:
        from_name (str): "From" property of email message.
    Returns:
        (str): Sanitized version of name suitable for use as part of a
               filename.
    """
    result = from_name + ""

    # Strip any "<" from the beginning of the name
    if result[:1] == "<":
        result = result[1:]

    # Strip and ">" from the end of the name
    if result[-1:] == ">":
        result = result[:-1]

    return result


TEXT_LINK_REGEX = re.compile(r'(http|https)://(.*?\.pdf)', re.IGNORECASE)


def extract_text_links(text_content: str) -> list:
    """
    Extract a plain-text link passed to us. Email text content will probably
    be in quote-printable form (e.g. 3dutf-8). Therefore, the first step is
    to convert from quote-printable to a regular UTF-8 string. Next, search
    this string for links.

    From: https://www.mschweighauser.com/fast-url-parsing-with-python/

    Args:
        text_content (str): HTML content to parse.

    Returns:
        (list): List of links. List is empty is no links are found.
    """
    my_content = quopri.decodestring(text_content).decode('utf-8')
    return ["{}://{}".format(match[0], match[1])
            for match in TEXT_LINK_REGEX.findall(my_content)]


HTML_TAG_REGEX = re.compile(r'<a[^<>]+?href=([\'\"])(.*?)\1', re.IGNORECASE)


def extract_html_links(html_content: str) -> list:
    """
    Extract the value of the HREF property from any links ("a" tags) found in
    the html_content passed to us. Email text content will probably be in
    quote-printable form (e.g. 3dutf-8). Therefore, the first step is to
    convert from quote-printable to a regular UTF-8 string. Next, search this
    string for links.

    From: https://www.mschweighauser.com/fast-url-parsing-with-python/

    Args:
        html_content (str): HTML content to parse.

    Returns:
        (list): List of links. List is empty is no links are found.
    """
    my_content = quopri.decodestring(html_content).decode('utf-8')
    return [match[1] for match in HTML_TAG_REGEX.findall(my_content)]


def cloudize_link(link: str) -> str:
    """
    Modify the link, as necessary, if it points to a cloud service,
    e.g. DropBox.

    NOTE: This will **NOT** work if the cloud service link has been obscured
    through a service such as *tineyurl*.

    Args:
        link (str): Link to examine and possibly modify.

    Returns:
        (str): Cloud-compatible link
    """
    clouds = [{"url": "https://www.dropbox.com", "postfix": "?dl=1"}]

    for cloud in clouds:
        if link[:len(cloud["url"])] == cloud["url"]:
            return "{}{}".format(link, cloud["postfix"])

    return link


def _check_paths(path_envs: list):
    """
    Check files paths, identified by environment variables.
    Create missing paths.

    Args:
        path_envs (list): List of environment variables defining paths

    Returns:
        (bool): True is all is OK, otherwise False
    """
    for path_env in path_envs:
        try:
            path = os.environ.get('path_env', None)
            if path:
                os.makedirs(path, exist_ok=True)
        except Exception as e:
            print(f"Error create path for '{path_env}' ({path}): {str(e)}")
            return False
    return True


def main():
    logger_obj = Logger()
    logger = logger_obj.get_logger(MAIN)
    lock = ServerLock(LOCK_PORT)
    if not lock.lock():
        logger.error(
            "{} is running elsewhere. Quitting this copy.".format(__file__)
        )
        exit()

    mailgetter = MailGetter()

    if not mailgetter.connect():
        logger.error("Unable to connect to email server. Exiting.")
        exit()

    if not mailgetter.check_folders():
        logger.error("Cannot continue with incorrect folder structure.")
        exit()

    try:
        mailgetter.wait_for_messages()
    except KeyboardInterrupt:
        print("Good bye")


if __name__ == "__main__":
    main()
