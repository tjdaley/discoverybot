"""
logger.py - Logging convenience

Copyright (c) 2019 by Thomas J. Daley, J.D. All Rights Reserved.
"""
import logging


class Logger(object):
    """
    A convenience class for instantiating a consistent logger.
    """
    @staticmethod
    def get_logger(
        log_name: str = __name__,
        file_log_level: int = logging.DEBUG,
        console_log_level: int = logging.DEBUG
    ):
        """
        Return a logger.
        """
        my_logger = logging.getLogger(log_name)
        my_logger.setLevel(logging.DEBUG)
        file_handler = logging.FileHandler(log_name+".log")
        file_handler.setLevel(file_log_level)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(console_log_level)
        template = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        formatter = logging.Formatter(template)
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        my_logger.addHandler(file_handler)
        my_logger.addHandler(console_handler)
        return my_logger
