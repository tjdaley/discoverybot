"""
serverlock.py - A server-wide lock.

Copyright (c) 2019 by Thomas J. Daley, Esq. All Rights Reserved.
"""
import socket


class ServerLock(object):
    """
    Implements a server-wide lock.
    """
    def __init__(self, lock_port: int):
        """
        Class initializer.

        Args:
            lock_port (int): TCP port to use as our server-wide lock.
        """
        self.lock_port = lock_port
        self.socket = None

    def lock(self) -> bool:
        """
        Attempt to obtain a server-wide lock.

        Args:
            None.

        Returns:
            (bool): True if lock obtained, otherwise False
        """
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.bind(('localhost', self.lock_port))
            self.socket = s
        except OSError:
            return False

        return True

    def release(self):
        """
        Release the lock obtained through lock().
        """
        if self.socket is None:
            return True

        try:
            self.socket.close()
        except Exception as e:
            print(str(e))
            pass

        return True
