import logging
import os.path
import sys
import importlib
import argparse
from .base import Agent, index_split, DEFAULT_PORT


class DynamicAgent(Agent):
    def __init__(self, path):
        super(DynamicAgent, self).__init__()
        self.module_cache = {}
        sys.path.insert(1, os.path.abspath(path))

    def look_up_function(self, index):
        if index not in self.function_store:
            module_name, func_name = index_split(index)
            if module_name not in self.module_cache:
                self.module_cache[module_name] = importlib.import_module(module_name)
            self.register(getattr(self.module_cache[module_name], func_name), index)
        return self.function_store[index]

    @classmethod
    def launch(cls):
        parser = argparse.ArgumentParser()
        parser.add_argument("path", type=str, help="path", nargs="?", default=".")
        parser.add_argument("-p", "--port", type=int, help="port", default=DEFAULT_PORT)
        parser.add_argument("-l", "--logging", type=str, help="Logging level", default="warning")
        args = parser.parse_args()
        logging.basicConfig(level=getattr(logging, args.logging.upper()))
        cls(args.path).run(port=args.port)
