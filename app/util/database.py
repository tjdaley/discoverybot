"""
database.py - Class for access our persistent data store for discoverybot.

Copyright (c) 2019 by Thomas J. Daley, J.D. All Rights Reserved.
"""
__author__ = "Thomas J. Daley, J.D."
__version__ = "0.0.1"

from datetime import datetime
import time

from pymongo import MongoClient

from .params import Params
from .logger import Logger

DB_URL = "mongodb://ec2-54-235-51-13.compute-1.amazonaws.com:27017/"
DB_NAME = "discoverybot"
FILE_TABLE_NAME = "received_files"

class Database(object):
    """
    Encapsulates a database accessor that is agnostic as to the underlying
    database product or implementation, e.g. mongo, mysql, dynamodb, flat files, etc.
    """
    def __init__(self):
        """
        Instance initializer.
        """
        self.client = None
        self.dbconn = None
        self.logger = Logger.get_logger()
        self.last_inserted_id = None

    def connect(self)->bool:
        """
        Connect to the underlying datastore.

        Returns:
            (bool): True if successful, otherwise False.
        """
        success = False

        try:
            self.logger.debug("Connecting to database %s at %s", DB_NAME, DB_URL)
            client = MongoClient(DB_URL)
            dbconn = client[DB_NAME]
            self.client = client
            self.dbconn = dbconn
            self.logger.info("Connected to database.")
            success = True
        except Exception as e:
            self.logger.error("Error connecting to database: %s", e)
        
        return success

    def insert_received_file(self, **kwargs)->bool:
        """
        Record the fact that we have received a file.

        Args:
            from (str): Sender's email address.
            reply_to (str): Reply-to email address.
            filepath (str): Full path to the file name.
        
        Returns:
            (bool): True if successful, otherwise False
        """
        record = record_from_kwargs(kwargs)
        record["status"] = "N"
        record["status_time"] = time.time()

        id = self.dbconn[FILE_TABLE_NAME].insert_one(record).inserted_id
        self.last_inserted_id = id
        return True

def base_record()->dict:
    """
    Return a basic record with the audit flags we use in all records.

    Args:
        None

    Returns:
        (dict): dict with audit fields populated.
    """
    return {"time": time.time(), "time_str": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")}

def record_from_kwargs(kwargs:dict)->dict:
    """
    Create a record from kwargs dict.

    Args:
        kwargs (dict): Dict of key-word args from caller.
    
    Returns:
        (dict): Standardized record.
    """
    record = base_record()
    for key, value in kwargs.items():
        record[key] = value

    return record