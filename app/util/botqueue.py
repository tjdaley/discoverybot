"""
queue.py - Implement a queue through MongoDb

Copyright (c) 2019 by Thomas J. Daley, J.D. All Rights Reserved.
"""
from datetime import datetime
import json
import os
import redis

import util.env
from .logger import Logger


class BotQueue(object):
    """
    Encapsulates a queue that is agnostic as to the underlying
    database product or implementation, e.g. mongo, mysql, dynamodb, flat
    files, etc.
    """
    def __init__(self, queue_name: str = "queue", ttl: int = 60*60*24):
        """
        Instance initializer.
        """
        self.queue = redis.Redis(host=os.environ.get('REDIS_SERVER'))
        self.queue_name = os.environ.get('DISCOVERY_BOT_IN', 'discoverybot-in')
        self.logger = Logger.get_logger()

    def open(self) -> bool:
        """
        Connect to the underlying datastore.

        Returns:
            (bool): True if successful, otherwise False.
        """
        raise Exception('Unsupported method: BotQUeue.open()')

    def publish(self, record: dict, priority: int = 5) -> bool:
        """
        Write an entry to the queue.

        Args:
            record (dict): The item to be queued.
            priority (int): The priority of the item. Default = 5. Lesser
                values are processed before greater values. I.E. priority=1
                is processed before priority=5 which is processed before
                priority=10. (IGNORED)

        Returns:
            (bool): True if successful, otherwise False.
        """
        success = True
        item = {
                "created_at": datetime.now().strftime('%Y-%m-%d'),
                # "started_at": zero_date(),
                # "completed_at": zero_date(),
                # "priority": priority,
                "payload": record
        }
        try:
            self.queue.lpush(self.queue_name, json.dumps(item))
        except Exception as e:
            self.logger.error("Error queueing record: %s", str(e))
            success = False

        return success

    def next(self, block: bool = True) -> dict:
        """
        Await an item from the queue.
        """
        if not block:
            message = self.queue.brpop(self.queue_name)
            if message:
                return json.loads(message[1].decode())
            return {}

        try:
            while True:
                message = self.queue.brpop(self.queue_name, timeout=30)
                if message:
                    return json.loads(message[1].decode())
        except Exception as e:
            self.logger.exception(e)
            self.logger.error("Error dequeuing item: %s", e)

    def finish(self, item: dict) -> bool:
        """
        Mark a job as finished.

        Args:
            item (dict): Item that was returned by next() or the iterator.

        Returns:
            (bool): True if successful, otherwise False.
        """
        return True

    def count(self) -> int:
        """
        Returns:
            (int): Number of items in the queue.
        """
        return self.queue.llen(self.queue_name)

    def clear(self) -> bool:
        """
        Clear all items from the queue.

        Returns:
            (bool): True if successful, otherwise False.
        """
        self.queue.ltrim(self.queue_name, 1, 0)


def zero_date():
    """
    The size of a record in a capped collection cannot be changed
    once it is first committed to the collection. Normally we would
    store ```None``` or ```0``` in the "empty" date fields, but then
    when we update the record with the correct date/time, the record
    size would change. This function creates a meaningless date value
    that acts as a placeholder until we have a meaningful falue later.
    """
    return datetime.fromordinal(1)


if __name__ == "__main__":
    queue = BotQueue()
    queue.publish({"text": "Hello, World!"})
    item = queue.next()
    print(item["text"])
    queue.finish(item)
    print("Waiting for next item.")
    item = queue.next()
