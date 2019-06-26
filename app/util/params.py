"""
params.py - Class for reading and serving run-time parameters to the app.

EXAMPLES:

1. Runtime parameters are in a file called "params.json":

    from params import Params
    my_params = Params()
    # Thereafter, use my_params like a dict object.

2. Runtime parameters are in a file called "my_params.json":

    from params import Params
    my_params = Params(param_file="my_params.json")
    # Thereafter, use my_params like a dict object.

3. Runtime parameters are in these environment variables:

    MYAPP_username = "tdaley"
    MYAPP_password = "my password"
    MYAPP_server   = "localhost"

    from params import Params
    my_params = Params(storage="env", param_prefix="MYAPP_")
    # Thereafter, use my_params as a dict object where the keys
    # are the environment variable names with the prefix removed, e.g.
    print(my_params.keys)
    # KeysView({"username":"tdaley", "password":"my password", "server":"localhost"})

Copyright (c) 2019 by Thomas J. Daley, J.D. All Rights Reserved.
"""
__author__ = "Thomas J. Daley, J.D."
__version__ = "0.0.1"

import json
import os
from collections import UserDict

PARAM_PREFIX = "dbot_"
PARAM_FILE = "params.json"

class Params(UserDict):
    """
    Encapsulates parameters behavior. Isolates app components from parameter
    storage implementation (json, DB, env, etc.).
    """
    def __init__(self, storage:str = "json", param_prefix:str = PARAM_PREFIX, param_file:str = PARAM_FILE)->dict:
        """
        Class initializer. Reads params from storage.

        Args:
            storage (str): Type of storage. Options are "json" and "env", for now.
                        Default is "json".
            param_prefix (str): Prefix to be used in filtering environment variables.
                                Only applicable if *storage* = "env".
            param_file (str): Name of file to read for parameters. Default = params.json.
                              Only applicable if *storage* = "json"
        """
        self.data = {}
        if storage == "json":
            self.__read_json_params(param_file)
        elif storage == "env":
            self.__read_environment_params(param_prefix)
        else:
            raise ValueError("Storage must be one of 'json' or 'env' [{}].".format(storage))

    def __read_json_params(self, param_file):
        """
        Read run-time parameters from a json file called params.json.

        Returns:
            (dict): Dictionary of runtime parameter values.
        """
        with open(param_file, "r") as params_file:
            params = json.load(params_file)

        self.data = params

    def __read_environment_params(self, param_prefix):
        """
        Read run-time parameters from environment variables. Do this by looping through
        every environment variable and extracting those that begin with PARAM_PREFIX.

        Args:
            param_prefix (str): Prefix to be used in filtering environment variables.

        Returns:
            (dict): Dictionary of runtime parameter values.
        """
        prefix_length = len(param_prefix)

        for param, value in os.environ.items():
            if param[:prefix_length] == param_prefix:
                self.data[param[prefix_length:]] = value
