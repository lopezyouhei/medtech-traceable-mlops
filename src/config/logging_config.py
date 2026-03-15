from enum import Enum


class LogLevel(str, Enum):
    FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
