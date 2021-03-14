"""
textextractor.py - Extract text from PDF files.

Copyright (c) 2019 by Thomas J. Daley, J.D. All Rights Reserved.
"""
import ntpath
import socket
import time

from util.botqueue import BotQueue
from util.database import Database
from util.logger import Logger
from util.texasbarsearch import TexasBarSearch
import util.env  # noqa

from bson import json_util
import json

import re

# For GooglePdfTextExtractor
from apiclient import errors
from apiclient.http import MediaFileUpload, MediaIoBaseDownload
import mimetypes
import os
import io
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2 import service_account

# For sending email replies
import smtplib
import ssl
from email.message import EmailMessage

DEBUG = (os.environ.get('DEBUG', '0') == '1')
MAIN = "TXTEXT"


class TextExtractor(object):
    """
    Base class for text abstrators.
    """
    def __init__(self):
        pass

    def extract(self, filename: str, mime_type: str, **args: list) -> str:
        """
        Extract text from the given file. Returns the extracted text.

        Args:
            filename (str): Path to the file we are processing.
            mime_type (str): MIME-type for this file, e.g. 'application/pdf'.
            args (list): Some implementations may allow additional arguments.
        """
        pass


class GoogleTextExtractor(TextExtractor):
    """
    Load the file to GoogleDocs, then retrieve the text.
    """
    def __init__(self):
        """
        Class initializer.
        """
        TextExtractor.__init__(self)
        mimetypes.init()
        logger = Logger()
        self.logger = logger.get_logger(MAIN + ".GoogTxtExt")
        self.drive_service = self.__instantiate_drive_service()
        self.docs_service = self.__instantiate_docs_service()
        if not _check_paths(['processed_path']):
            self.logger.fatal("File paths could not be created...cannot continue.")
            exit()

    def extract(
        self,
        filename: str,
        mime_type: str = 'application/pdf'
    ) -> str:
        """
        Extract text from PDF file.
        """
        file = self.__upload_file(filename, mime_type)
        file_id = file.get('id')
        content = self.__get_file_content(file_id)
        if not DEBUG:
            success = self.__remove_file(file_id)  # noqa
        return content

    def __instantiate_drive_service(self):
        """
        Build a Google Docs service object.

        From: https://blog.benjames.io/2020/09/13/authorise-your-python-google-drive-api-the-easy-way/
        """
        self.logger.info("Authorizing drive service.")
        scopes = ['https://www.googleapis.com/auth/drive']
        creds = self.__build_google_credentials(scopes)
        service = build('drive', 'v3', credentials=creds)
        return service

    def __instantiate_docs_service(self):
        """
        Build a Google Docs service object.

        From: https://blog.benjames.io/2020/09/13/authorise-your-python-google-drive-api-the-easy-way/
        """
        self.logger.info("Authorizing docs service")
        scopes = ['https://www.googleapis.com/auth/documents']
        creds = self.__build_google_credentials(scopes)
        service = build('docs', 'v1', credentials=creds)
        return service

    def __build_google_credentials(self, scopes: list):
        """
        Build google credentials for using a service account (for a server-based app).

        Args:
            scopes (list): List of APIs that we want access to.

        Returns:
            Google Credentials
        """
        with open('google_credentials.json', 'r') as f:
            service_account_info = json.load(f)
        creds = service_account.Credentials.from_service_account_info(service_account_info, scopes=scopes)
        return creds

    def __upload_file(
        self,
        filename: str,
        src_mime_type: str = None,
        dest_mime_type: str = 'application/vnd.google-apps.document'
    ):
        """
        Upload a file to Google Drive.

        Args:
            filename (str): Name of the file to upload. This is the path of the
                file that is to be uploaded.
            src_mime_type (str): MIME-type string of the source document
            dest_mime_type (str): MIME-type of the document type that is to be
                created. This how we get Google Docs to do the conversion.
                default='application/vnd.google-apps.document'

        Returns:
            File resource or None.
        """
        self.logger.debug("Uploading %s", filename)

        if src_mime_type is None:
            file_type = filename[filename.rindex("."):].lower()
            try:
                src_mime_type = mimetypes.types_map[file_type]
            except KeyError:
                self.logger.error("Unable to determine mime type for '%s'.", file_type)
                return None

        file_metadata = {
            'name': base_file_name(filename),
            'mimeType': dest_mime_type,
        }
        media = MediaFileUpload(filename, mimetype=src_mime_type)

        file_resource = None
        retry_count = 0

        while file_resource is None and retry_count < 3:
            try:
                file_resource = self.drive_service.files().create(
                    body=file_metadata,
                    media_body=media,
                    fields='id'
                ).execute()

                self.logger.info(
                    "Uploaded %s as file id %s.",
                    base_file_name(filename),
                    file_resource.get('id')
                )
            except HttpError as error:
                retry_count += 1
                self.logger.error("Error uploading %s: %s", base_file_name(filename), error)  # NOQA
                time.sleep(10*retry_count)

        return file_resource

    def __get_file_content(self, file_id: str) -> str:
        """
        Retrieve a file's content.

        From: https://developers.google.com/drive/api/v2/reference/files/get

        Args:
            file_id: ID of the file to retrieve metadata for

        Returns:
            (str): Plain text content of file.
        """
        self.logger.debug("Retrieving file contents")
        try:
            request = self.drive_service.files()\
                .export_media(fileId=file_id, mimeType='text/plain')
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
                self.logger.debug("Download %s%%", str(status.progress()*100))

            # See: https://stackoverflow.com/questions/17912307/u-ufeff-in-python-string # NOQA
            content = fh.getvalue().decode('utf-8-sig')
            return content
        except errors.HttpError as error:
            self.logger.error("Error retrieving file content: %s", error)

        return None

    def __remove_file(self, file_id: str) -> bool:
        """
        Remove a file from Google Drive.

        Args:
            file_id (str): ID of the file to remove.

        Returns:
            (bool): True if successful, otherwise False.
        """
        self.logger.debug("Removing file")
        try:
            self.drive_service.files().delete(fileId=file_id).execute()
            return True
        except errors.HttpError as error:
            self.logger.error("Error deleting file: %s", error)
        return False


class TextParser(object):
    """
    Parse the discovery requests from the text of a file.
    """
    def __init__(self):
        """
        Initializer.
        """
        self.lines = []
        logger = Logger()
        self.logger = logger.get_logger(MAIN + ".Parser")

    def init(self, text: str) -> int:
        """
        Takes a block of text, *text*, and preprocesses it for use
        by the other methods of this class.

        Args:
            text (str): Text block to process.
        Returns:
            (int): Number of lines of text loaded.
        """
        try:
            my_text = text.replace('\r', '\n')
            lines = my_text.split('\n')  # tjd on 2021-03-14 text.split('\n')
            self.lines = lines
        except Exception as error:
            self.logger.error("Error loading text: %s", error)

        return len(self.lines)

    def dump_lines(self, filename: str):
        line_number = 0
        with open(filename, "w") as out_file:
            for line in self.lines:
                line_number += 1
                out_line = "{} {} | {}\n".format(
                    ("000000" + str(line_number))[-6:],
                    ("      " + str(len(line)))[-6:],
                    line
                )
                out_file.write(out_line)

    def cause_number(self) -> str:
        """
        Parse out the cause number and return it.

        For now, assumes the last word of the first line is the cause number.
        """
        prefix_words = [
            'NO', 'NUM', 'NUMBER', 'CAUSE', 'CASE', 'MATTER',
        ]
        result = None
        lines_to_examine = 30
        line_index = 0
        while line_index < lines_to_examine and result is None:
            try:
                # split line into words containing only letters and numbers
                line = self.lines[line_index]
                clean_words = re.sub(r'[^a-zA-Z0-9\s]', '', line).split()
                # split line into words in their original form
                words = line.split()
                line_index += 1

                # If we found multiple words and one is a normal prefix word
                # for a cause number, then treat the final word of that row
                # as the cause number.
                if len(clean_words) > 1:
                    if clean_words[0].upper() in prefix_words:
                        result = words[-1]
                    elif clean_words[0].isdigit():
                        # If there is only one word on the line and it is more or
                        # less numeric, e.g. 470-5555-2019, treat that as the cause
                        # number.
                        result = words[0]
            except Exception as error:
                self.logger.error("Error extracting cause number: %s", error)
                self.logger.exception(error)
                return None

        return result

    def get_match_group(
        self,
        regexes: list,
        group_num: int = 1,
        window_size: int = 50
    ) -> str:
        """
        Search the first *line_count* lines using the provided *regexes*
        and, when a match is detected, return what is found in
        match group #*group_num*.

        Args:
            regexes (list): The regexes to use for searching. NOTE: They must
                be constructed so that the target text is in the same
                match group for each regex.
            group_num (int): Number of the group containing the target text.
            window_size (int): The number of lines to search within the text
                we loaded in *init()*. If set to None, all lines are searched.

        Returns:
            (str): The located text or None.
        """
        result = None
        line_count = window_size or len(self.lines)-1
        try:
            window = [line.upper() for line in self.lines[:line_count]]
            for pattern in regexes:
                for line in window:
                    match = re.search(pattern, line)
                    if match:
                        result = match.group(group_num).strip()
                        # print("*"*80, "\nLine:", line, "\nPattern:", pattern, "\nResult:", result, "\n", "|"*80)
                        break
                if result:
                    break
        except Exception as error:
            self.logger.error("Error searching: %s", error)
        return result

    def court_number(self) -> str:
        """
        Try to figure out what court number this cause belongs to.
        """
        court_patterns = [
            r'(\d+)(?:ND|RD|TH|ST)',
            r'([0-9]{1,3})\sJUDICIAL',
            r'(?:NO|NUMBER|NO\.|NUM|NUM\.)(?:\:|\s)(\d+)',
        ]
        return self.get_match_group(court_patterns, window_size=20)

    def county(self) -> str:
        """
        Try to figure out what county this case is in.
        """
        county_patterns = [
            r'([A-Z][A-Z\s\-]+[A-Z]+)\s*COUNTY,\s*TEXAS',
        ]
        return self.get_match_group(county_patterns)

    def oc_bar_number(self) -> str:
        """
        Look for opposing counsel's bar number.
        """
        bar_patterns = [
            r'(?:BAR NO\.|BAR NO|BAR NUM\.|BAR NUM|SBN|BAR NUMBER|BAR #).*(\d{8})',  # NOQA
        ]
        return self.get_match_group(bar_patterns, window_size=None)

    def oc_email(self) -> str:
        """
        Look for opposing counsel's email address. Beware not to grab an email
        address belonging to the responding party's attorney, which may appear
        within the "TO" line of the discovery request.
        """
        email_patterns = [
            r'^(\S+@\S+\.\S+)$',
            r'(\S+@\S+\.\S+)$',
            r'^(\S+@\S+\.\S+)\s',
            r'STATE BAR\s.*\s(\S+@\S+\.\S+)',
        ]
        return self.get_match_group(email_patterns, window_size=None)

    def categorize(self, categories: dict, window_size: int = 50) -> str:
        """
        Determine a category.

        Args:
            categories (dict): Key-Value pair of search-strings and labels.
            window_size (int): The number of lines to search within the text
                we loaded in *init()*.
        Returns:
            (str): Label text or None.
        """
        result = None
        window = [line.upper() for line in self.lines[:window_size]]
        for search_string, category_label in categories.items():
            for line in window:
                if search_string in line:
                    result = category_label
                    break
            if result is not None:
                break
        return result

    def court_type(self) -> str:
        """
        Try to figure out what type of court we are in.
        """
        searches = {
            "COUNTY COURT": "County Court At Law",
            "PROBATE COURT": "Probate Court",
            "DISTRICT COURT": "District Court",
            "JUSTICE COURT": "Justice Court",
            "JP COURT": "Justice Court",
        }
        return self.categorize(searches)

    def discovery_type(self) -> str:
        """
        Try to figure out what kind of discovery request this is.
        """
        searches = {
            "PRODUCTION AND INSPECTION": "PRODUCTION",
            "PRODUCTION & INSPECTION": "PRODUCTION",
            "REQUEST FOR PRODUCTION": "PRODUCTION",
            "REQUESTS FOR PRODUCTION": "PRODUCTION",
            "PRODUCTION REQUEST": "PRODUCTION",
            "PRODUCTION REQUESTS": "PRODUCTION",
            "INTERROGATORIES": "INTERROGATORIES",
            "RULE 197.2(D)": "INTERROGATORIES",
            "REQUEST FOR DISCLOSURES": "DISCLOSURES",
            "REQUESTS FOR DISCLOSURE": "DISCLOSURES",
            "RULE 194": "DISCLOSURES",
            "RULE OF CIVIL PROCEDURE 194": "DISCLOSURES",
            "REQUEST FOR ADMISSION": "ADMISSIONS",
            "REQUESTS FOR ADMISSION": "ADMISSIONS",
        }
        return self.categorize(searches)

    def discovery_requests(self) -> list:
        """
        Create a list of discovery requests from the provided text.

        Args:
            None.

        Returns:
            (list): List of strings where each string is a discovery
                request.
        """
        requests = []
        lines = self.lines
        starting_index, pattern = self.__first_request(lines)

        # We couldn't find the first request. Give up.
        if starting_index is None:
            return requests

        # We found the first request. Now find the rest.
        target_request_number = 1
        request_text = ''
        for line in lines[starting_index:]:
            next_request_id = pattern.format(str(target_request_number+1))
            cleaned_line = str(line).strip().upper().replace(' .', '.')

            # If the current line starts with the next request id that we
            # expect, complete the current request and save it.
            if cleaned_line.startswith(next_request_id):  # and cleaned_line[-1] not in ',;ND':  # NOQA
                # Save the request we've been working on.
                requests.append(self.__request_package(target_request_number, request_text))  # NOQA
                target_request_number += 1
                request_text = line
            elif line.strip().upper() != 'RESPONSE:':  # Filter out the ProDoc template
                request_text += line

        # Add the last request
        request_id = pattern.format(str(target_request_number))
        if str(request_text).strip().upper().startswith(request_id):
            requests.append(self.__request_package(target_request_number, request_text))  # NOQA

        return requests

    def __next_request(self, lines: list, request_num: int) -> (int, str, int):
        """
        Locate the next discovery request.

        Args:
            lines (list): Lines to search through.
            request_num (int): Request number we are searching for.

        Returns:
            (int): Index into *lines* if found, otherwise None.
            (str): The request
            (int): The number of the request
        """
        pass

    def __first_request(self, lines: list) -> (int, str):
        """
        Locate the first discovery request.

        Args:
            Lines of text to search.

        Returns:
            (int): Index into *lines* if found, otherwise None.
            (str): Prefix that was detected.
        """
        # From the end of the content, looking back, try to find the
        # first discovery request. The number of the request may have
        # a prefix, so try some common prefixes.
        #
        # The most common prefix is '' HOWEVER: If the attorney used a
        # different prefix and included a numbered list of instructions,
        # we'll get tricked into thinking the instructions are discovery
        # requests if we start with the no-prefix assumption.

        # NOTE: Trailing space it important!!
        request_patterns = ['REQUEST {}.', 'REQUEST {}:', 'INTERROGATORY {}.', 'INTERROGATORY {}:', '{}. Produce', '{}.']
        starting_index = None

        for pattern in request_patterns:
            request_id = pattern.format('1')
            self.logger.info("Checking for %s", request_id)
            for index, line in reversed(list(enumerate(lines))):
                test_line = str(line).strip().upper()

                # See if the line starts with the request id we're looking
                # for AND make sure it does NOT end with some punctuation
                # that suggests it's part of an internal sub-list.
                #
                # For example, the following text lines would NOT need the
                # second test because the list numbering would NOT fool us:
                #
                # 1. For each job you've had in the past 12 years, state:
                #    A. The name of the employer; and
                #    B. Your salary;
                #
                # Yet the following text lines WOULD need the second test
                # because the list numbering would fool us:
                #
                # 1. For each job you've had in the past 12 years, state:
                #    1. The name of the employer; and
                #    2. Your salary.
                #
                # That's the point of the second test that eliminates lines
                # ending with ";", ",", (an)"d", or (o)"r".
                if test_line.startswith(request_id) \
                   and test_line[-1] not in ',;DR':
                    # Request 1 is found
                    starting_index = index
                    break
            if starting_index is not None:
                break
        return starting_index, pattern

    def __request_package(
        self,
        request_number: int,
        request_text: str
    ) -> dict:
        """
        Package the discovery request into a dict. Cleans the *request_text* by
        removing leading/trailing white space and compressing consecutive
        white space.

        Args:
            request_number (int): The discovery request number (1, 2, 3, etc.)
            request_text (str): The text of the discovery request.

        Returns:
            (dict): Normalized packaging of the discovery request.
        """
        return {
            'number': request_number,
            'request': clean_string(request_text)
        }


class EmailNotifier(object):
    """
    Encapsulates ability to send a notification to the person who
    emailed the discovery requests to us.
    """
    SUBJECT = "{discovery_type} from {oc_name}"
    MESSAGE = """
    Cause #{cause_number}
    Court: {court_number} {court_type}
    County: {county}
    Discovery type: {discovery_type}
    Number of requests: {request_count}
    Propounded by:
    \t{oc_name}, Esq.
    \t{oc_email}
    \t{oc_address}
    \tState Bar No. {oc_bar_number}

    Sent from: {server}

    You can view these requests here: http://www.discovery.jdbot.us

    Kindest regards,

    Discovery Bot
    """

    def __init__(self):
        """
        Initializer.
        """
        pass

    def reply(self, doc: dict) -> bool:
        """
        Send a reply to the user who sent the requests to us.

        Args:
            doc (dict): The document that was saved to the database.
        Returns:
            (bool): True if successful, otherwise False.
        """
        subject, content = self.format_message(doc)
        message = EmailMessage()
        message['From'] = os.environ.get('mail_username')
        message['To'] = doc['item']['payload']['reply_to']
        message['Subject'] = subject
        message['Bcc'] = 'tom@powerdaley.com'
        message.set_content(content)
        # message.preamble = 'MIME Message Preamble'
        # message.add_attachment(message, maintype='text', subtype='plain')
        # message.attach(MIMEText(message, 'plain'))
        # message = message.as_string()

        mailserver = os.environ.get('mailserver')
        smtpport = os.environ.get('smtpport')
        username = os.environ.get('mail_username')
        password = os.environ.get('mail_password')

        try:
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(mailserver, smtpport, context=context) as server:
                server.login(username, password)
                # server.sendmail(username, doc['item']['payload']['reply_to'], message)
                server.send_message(message)
                # server.quit()
        except Exception as error:
            print("Error sending email:", str(error))

    def format_message(self, doc: dict) -> (str, str):
        """
        Format an outbound message.

        Args:
            doc (dict): Document that was saved to the datbase.
        Returns:
            (str): Subject
            (str): Message
        """
        oc = doc.get('requesting_attorney', {})
        oc_bar_number = oc.get('bar_number', "unknown")
        oc_email = oc.get('email', "unknown")
        oc_details = oc.get('details', {})
        oc_name = oc_details.get('name', "unknown")
        oc_address = oc_details.get('address', "unknown")

        message = EmailNotifier.MESSAGE.format(
            discovery_type=doc['discovery_type'],
            oc_name=oc_name,
            cause_number=doc['cause_number'],
            court_number=doc['court_number'],
            court_type=doc['court_type'],
            county=doc['county'],
            request_count=len(doc['requests']),
            oc_email=oc_email,
            oc_address=oc_address,
            oc_bar_number=oc_bar_number,
            server=doc.get('server', 'unknown'),
        )
        subject = EmailNotifier.SUBJECT.format(
            discovery_type=doc['discovery_type'],
            oc_name=oc_name,
            cause_number=doc['cause_number'],
            court_number=doc['court_number'],
            court_type=doc['court_type'],
            county=doc['county'],
            request_count=len(doc['requests']),
            oc_email=oc_email,
            oc_address=oc_address,
            oc_bar_number=oc_bar_number,
        )
        return subject, message


def clean_string(instr: str) -> str:
    """
    Clean up a string by removing multiple consecutive spaces,
    trimming, and replacing certain unicode escapes sequences with ASCII
    equivalents.

    Args:
        instr (str): String to be cleaned.
    Returns:
        (str): Cleaned string.
    """
    result = compress_whitespace(instr)
    result = replace_unicode(result)
    return result


def compress_whitespace(instr: str) -> str:
    """
    Remove leading and trailing white space and compress multiple
    consecutive internal spaces into one space.

    Args:
        instr (str): String to be cleaned.
    Returns:
        (str): The cleaned string.
    """
    result = instr.strip()
    result = ' '.join(result.split())
    return result


def replace_unicode(instr: str) -> str:
    """
    Replace unicode-looking escape sequences with an ASCII equivalent.

    Args:
        instr (str): Input string.
    Returns:
        (str): Cleaned string.
    """
    replacements = {
        '\u2010': '-',
        '\u2011': '-',
        '\u2012': '-',
        '\u2013': '-',
        '\u2014': '--',
        '\u2015': '--',
        '\u2018': "'",
        '\u2019': "'",
        '\u201b': "'",
        '\u201c': '"',
        '\u201d': '"',
        '\u201f': '"',
    }
    result = str(instr)
    for old, new in replacements.items():
        result = result.replace(old, new)
    return result


def base_file_name(filename: str) -> str:
    """
    Extracts just the file name from a string that
    might contain a path.

    This will work on NT-based systems and *nix systems.
    It will work if the filename ends in a path delimter, e.g.
        "/home/tom/myfile.pdf/"
    It will NOT work if the filename contains escaped Unicode
    characters.

    *See*: https://stackoverflow.com/questions/8384737/extract-file-name-from-path-no-matter-what-the-os-path-format # NOQA

    Args:
        filename (str): Filename to parse.

    Returns:
        (str): Base file name.
    """
    escaped_file_name = filename.replace("\\", "\\\\")
    path_name, file_name = ntpath.split(escaped_file_name)
    return file_name or ntpath.basename(path_name)


def output_file_name(filename: str, path: str) -> str:
    """
    Create a name for our output file.

    Args:
        filename (str): Input file name.

    Returns:
        (str): Output file name to write to.
    """
    fn = base_file_name(filename)
    return "{}/{}.txt".format(
        path,
        fn[:fn.rindex(".")]
        )


def validate_item(item: dict, logger) -> bool:
    """
    Validates the dequeued item.

    Args:
        item (dict): The item to be validated.
        logger: Reference to our logger.

    Returns:
        (bool): True if OK, otherwise False.
    """
    if "payload" not in item:
        logger.error("Item has no payload: %s", item)
        return False

    if "filename" not in item["payload"]:
        logger.error("Payload has no filename: %s", item["payload"])
        return False

    return True


def get_email(s: str) -> str:
    """
    Extract just the email address from an address that may
    contain a display name, e.g. "Thomas Daley" <tjd@powerdaley.com>

    Args:
        s (str): String to search

    Returns:
        (str): The email address we located or, if none located, the
               original string.
    """
    match = re.search(r'([A-Za-z0-9\-\.\_\$]+@[A-Za-z0-9\-\.\_]+\.[A-Za-z]+)', s)
    if match:
        return match.group(1).strip()
    return s


def _check_paths(path_envs: list):
    """
    Check files paths, identified by environment variables.
    Create missing paths.

    Args:
        path_envs (list): List of environment variables defining paths

    Returns:
        (bool): True is all is OK, otherwise False
    """
    print("Verifying file paths.")
    for path_env in path_envs:
        try:
            path = os.environ.get(path_env, None)
            print(f"\tVerifying path {path_env} as {path}")
            if path:
                os.makedirs(path, exist_ok=True)
                print(f"\tVerfied path: {path}")
        except Exception as e:
            print(f"Error creating path for '{path_env}' ({path}): {str(e)}")
            return False
    return True


def main():
    queue = BotQueue()
    db = Database()
    db.connect()
    attorney_searcher = TexasBarSearch()
    google_extractor = GoogleTextExtractor()
    emailer = EmailNotifier()
    processed_path = os.environ.get('processed_path')
    extractors = {
        "PDF": google_extractor,
        "DOCX": google_extractor,
        "DOC": google_extractor,
        "RTF": google_extractor,
        "TXT": google_extractor,
    }
    logger_obj = Logger()
    logger = logger_obj.get_logger(MAIN)
    parser = TextParser()

    while True:
        # Retrieve next item from queue. This call blocks.
        item = queue.next()

        # Ensure item is properly constructed
        if not validate_item(item, logger):
            queue.finish(item)
            continue

        # Extract filename from payload. This is the file we are extracting
        # text from
        filename = item["payload"]["filename"]

        # See if we know now to extract text from this type of file.
        file_type = filename[filename.rindex(".")+1:].upper()
        if file_type not in extractors:
            logger.debug("Unable to process %s file: %s", file_type, filename)
            queue.finish(item)
            continue

        # We have a handler...use it.
        extractor = extractors[file_type]
        logger.debug("Processing %s.", filename)
        text = extractor.extract(filename)

        # Parse out the discovery requests
        logger.debug("Parsing requests from %s.", filename)
        parser.init(text)
        requests = parser.discovery_requests()
        logger.debug("Extracted %s requests from %s", str(len(requests)), filename)  # NOQA
        if requests:
            if DEBUG:
                for request in requests:
                    # print("REQUEST {}: {}\n{}\n".format(request["number"], request["request"], "-"*80))  # NOQA
                    pass  # stop printing this while we debug other stuff. TJD 2021-03.14

            bar_number = parser.oc_bar_number()
            email_from = get_email(item['payload']['email_from'])
            doc = {
                'court_type': parser.court_type(),
                'court_number': parser.court_number(),
                'county': parser.county(),
                'cause_number': parser.cause_number(),
                'discovery_type': parser.discovery_type(),
                'owner': email_from,
                'server': socket.gethostname(),
                'requesting_attorney': {
                    'bar_number': bar_number,
                    'email': parser.oc_email(),
                    'details': attorney_searcher.find(bar_number)
                },
                'requests': requests,
                'item': item,
            }

            # Link to client record, if we can find it.
            client = db.get_client_id(parser.county(), parser.cause_number(), email_from)
            if client:
                doc['client_id'] = client['_id']

            db.insert_discovery_requests(doc)
            emailer.reply(doc)

            if DEBUG:
                outfile = output_file_name(filename, processed_path) + ".json"  # NOQA
                with open(outfile, "w") as json_file:
                    json.dump(doc, json_file, indent=4, default=json_util.default)  # NOQA
                parser.dump_lines(output_file_name(filename, processed_path) + "_dump.txt")  # NOQA

        # See if we got anything useful
        if text is not None:
            # Save extracted text
            outfile = output_file_name(filename, processed_path)
            with open(outfile, "w") as text_file:
                text_file.write(text)
        queue.finish(item)
        logger.debug("Processed %s to %s", filename, outfile)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Good bye.")
