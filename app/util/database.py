"""
database.py - Class for access our persistent data store for discoverybot.

Copyright (c) 2019 by Thomas J. Daley, J.D. All Rights Reserved.
"""
from datetime import datetime
import os
import time

from pymongo import MongoClient

import util.env
from .logger import Logger


class Database(object):
    """
    Encapsulates a database accessor that is agnostic as to the underlying
    database product or implementation, e.g. mongo, mysql, dynamodb,
    flat files, etc.
    """
    def __init__(self):
        """
        Instance initializer.
        """
        self.client = None
        self.dbconn = None
        self.client_conn = None
        self.logger = Logger.get_logger()
        self.last_inserted_id = None

    def connect(self) -> bool:
        """
        Connect to the underlying datastore.

        Returns:
            (bool): True if successful, otherwise False.
        """
        success = False

        try:
            db_name = os.environ.get('DB_NAME')
            db_url = os.environ.get('DB_URL')
            self.logger.debug("Connecting to db %s at %s", db_name, db_url)
            client = MongoClient(db_url)
            dbconn = client[db_name]
            self.client = client
            self.dbconn = dbconn
            self.client_conn = client['payment_redirect']
            self.logger.info("Connected to database.")
            success = True
        except Exception as e:
            self.logger.error("Error connecting to database: %s", e)
            self.logger.exception(e)

        return success

    def insert_discovery_requests(self, requests) -> bool:
        """
        Save discovery requests.

        Args:
            requests (dict): Requests create by *textextractor*.

        Returns:
            (bool): True if successful, otherwise False.
        """
        discovery_collection = os.environ.get('DISCOVERY_TABLE_NAME')
        record = base_record()
        record = dict(record, **requests)
        id = self.dbconn[discovery_collection].insert_one(record).inserted_id
        self.last_inserted_id = id
        return True

    def get_client_id(self, county: str, cause_number: str, email: str) -> str:
        """
        See if there is a matching client for this county and cause number combo.

        Args:
            county (str): Name of the county, all uppercase, without the word "COUNTY"
            cause_number (str): Cause Number, all uppercase
            email (str): Email of person who sent the discovery requests. Make sure
                they are authorized to connect discovery to this case.

        Returns:
            (id): MongoDB ID of corresponding client id.
        """
        query = {
            '$and': [
                {'case_county': {'$regex': f'^{county}$', '$options':'i'}},
                {'cause_number': {'$eq': cause_number}},
                {'admin_users': {'$elemMatch': {'$eq': email.lower()}}}
            ]
        }
        client = self.client_conn['clients'].find_one(query)
        return client


def base_record() -> dict:
    """
    Return a basic record with the audit flags we use in all records.

    Args:
        None

    Returns:
        (dict): dict with audit fields populated.
    """
    return {
        "time": time.time(),
        "time_str": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")}


def record_from_kwargs(kwargs: dict) -> dict:
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
