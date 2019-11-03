"""
queue.py - Implement a queue through MongoDb

Copyright (c) 2019 by Thomas J. Daley, J.D. All Rights Reserved.
"""
from datetime import datetime
import time

from pymongo import MongoClient, ASCENDING, DESCENDING, ReturnDocument

from .params import Params
from .logger import Logger

DB_URL = "mongodb://ec2-54-235-51-13.compute-1.amazonaws.com:27017/"
DB_NAME = "discoverybot"
FILE_TABLE_NAME = "received_files"

class BotQueue(object):
    """
    Encapsulates a queue that is agnostic as to the underlying
    database product or implementation, e.g. mongo, mysql, dynamodb, flat files, etc.
    """
    def __init__(self, queue_name:str = "queue", ttl:int = 60*60*24):
        """
        Instance initializer.
        """
        self.client = None
        self.dbconn = None
        self.collection = queue_name
        self.ttl = ttl
        self.logger = Logger.get_logger()
        self.open()
        self.queue = self.dbconn[self.collection]

    def verify_queue(self):
        """
        Make sure the queue collection is set up properly.
        """
        if not self.collection in self.dbconn.collection_names():
            self.dbconn.create_collection(self.collection, capped=True, max=100000, size=100000, autoIndexId=True)
            self.dbconn[self.collection].create_index([("started_at", ASCENDING)])
            self.dbconn[self.collection].create_index([("completed_at", ASCENDING)], expireAfterSeconds=self.ttl)

    def open(self)->bool:
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
            self.verify_queue()
            success = True
        except Exception as e:
            self.logger.error("Error connecting to database: %s", e)
        
        return success

    def publish(self, record:dict, priority:int = 5)->bool:
        """
        Write an entry to the queue.

        Args:
            record (dict): The item to be queued.
            priority (int): The priority of the item. Default = 5. Lesser values are
                processed before greater values. I.E. priority=1 is processed before
                priority=5 which is processed before priority=10.
        
        Returns:
            (bool): True if successful, otherwise False.
        """
        success = True
        item = {
                "created_at": datetime.now(),
                "started_at": zero_date(),
                "completed_at": zero_date(),
                "priority": priority,
                "payload": record
        }
        try:
            self.queue.insert(item, manipulate=False)
        except Exception as e:
            self.logger.error("Error queueing record: %s", str(e))
            success = False

        return success

    def next(self, block:bool = True)->dict:
        """
        Await an item from the queue.
        """
        query = {"started_at": zero_date()}
        order_by = [("priority", ASCENDING), ("created_at", ASCENDING)]

        retry_count = 0
        item = None

        while not item:
            item = self.queue.find_one_and_update(
                filter = query,
                sort = order_by,
                update = {"$set": {"started_at": datetime.now()}},
                tailable = True,
                return_document = ReturnDocument.AFTER
            )
            if not item:
                if retry_count < 5:
                    retry_count += 1
                time.sleep(2**retry_count)

        return item

    def finish(self, item:dict)->bool:
        """
        Mark a job as finished.

        Args:
            item (dict): Item that was returned by next() or the iterator.
        
        Returns:
            (bool): True if successful, otherwise False.
        """
        item["completed_at"] = datetime.now()
        self.queue.save(item)
        return True

    def count(self)->int:
        """
        Returns:
            (int): Number of items in the queue.
        """
        cursor = self.queue.find({"started_at": zero_date()})
        if cursor:
            return cursor.count()
        return 0

    def clear(self)->bool:
        """
        Clear all items from the queue.

        Returns:
            (bool): True if successful, otherwise False.
        """
        self.queue.drop()

def zero_date():
    return datetime.fromordinal(1)

if __name__ == "__main__":
    queue = BotQueue()
    queue.publish({"text": "Hello, World!"})
    item = queue.next()
    print(item["text"])
    queue.finish(item)
    print("Waiting for next item.")
    item = queue.next()