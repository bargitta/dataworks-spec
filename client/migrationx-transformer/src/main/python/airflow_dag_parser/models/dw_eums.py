from enum import Enum


class Architecture(str, Enum):
    TABLE = "Table"
    FILE = "File"
    NODE_OUTPUT = "NodeOutput"
    VARIABLE = "Variable"
