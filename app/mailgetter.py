"""
mailgetter.py - Retrieve email messages addressed to this service.

Copyright (c) 2019 by Thomas J. Daley, J.D. All Rights Reserved.
"""
__author__ = "Thomas J. Daley, J.D."
__version__ = "0.0.1"

import email
import mimetypes
import os

from imapclient import IMAPClient, exceptions as IMAPExceptions

from util.params import Params
from util.logger import Logger

class MailGetter(object):
    """
    Encapsulates behavior of retrieving email, saving PDFs, and moving emails.
    """
    
    def __init__(self):
        """
        Class initializer.
        """
        self.logger = Logger.get_logger()
        self.params = Params(param_file="./app/params.json")
        self.server = None

    def connect(self)->bool:
        """
        Connect and login to the remote IMAP server.

        Returns:
            (bool): True if successful, otherwise False
        """
        try:
            self.server = IMAPClient(self.params["mailserver"], port=self.params["mailport"], ssl=self.params["mailssl"], use_uid=True)
            self.server.login(self.params["username"], self.params["password"])
        except ConnectionRefusedError as e:
            self.logger.fatal("Connection to %s:%s was refused: %s", self.params["mailserver"], self.params["mailport"], e)
            return False

        return True

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
        if not self.server.folder_exists(self.params["inbox"]):
            self.logger.fatal("Error locating INBOX named '%s'.", self.params["inbox"])
            return False

        # Next, see if the PROCESSED folder exists. If it does not, try to create it.
        # If we try to create it and the creation fails, again, we have a serious problem and cannot
        # continue.
        if not self.server.folder_exists(self.params["processed_folder"]):
            self.logger.error("Error locating PROCESSED folder named '%s'.", self.params["processed_folder"])
            self.logger.error("Will attempt to create folder named '%s'.", self.params["processed_folder"])

            try:
                message = self.server.create_folder(self.params["processed_folder"])
                self.logger.info("Successfully created '%s': %s", self.params["processed_folder"], message)
            except Exception:
                self.logger.fatal("Failed to create '%s': %s", self.params["processed_folder"], message)
                return False

        self.logger.info("Folder check was successful.")
        return True

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
                if extension.upper() == ".PDF":
                    with open("{}/{}".format(self.params["input_path"], filename), "wb") as fp:
                        fp.write(part.get_payload(decode=True))
            except Exception as e:
                self.logger.error("Error processing attachment #%s from message #%s from %s: %s",
                    counter, msgid, message.get("From"), e)
                return False
            
        return True


    def process_inbox(self):
        """
        Process each message in the INBOX. After a message is processed:

            If processing was successful: Mark as "SEEN" so that we don't process it again.

            If processing was not successful: Mark as "SEEN" and "FOLLOWUP" so that an operator can fix
            if and requeue it by clearing the SEEN flag.
        """
        select_info = self.server.select_folder(self.params["inbox"])
        self.logger.debug("%d messages in %s.", select_info[b'EXISTS'], self.params["inbox"])

        messages = self.server.search(criteria='UNSEEN')

        for msgid, data in self.server.fetch(messages, ['RFC822']).items():
            email_message = email.message_from_bytes(data[b'RFC822'])

            if self.process_message(msgid, email_message):
                # Mark message as "Seen". For now, we *won't* move the message to the PROCESSED folder.
                self.server.set_flags(msgid, b'\\Seen')
            else:
                # If we had an error processing the message, mark it for follow AND as seen.
                self.server.set_flags(msgid, [b'\\Flagged for Followup', b'\\Seen'])

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

def main():
    mailgetter = MailGetter()
    if mailgetter.connect():
        if mailgetter.check_folders():
            mailgetter.process_inbox()

if __name__ == "__main__":
    main()
