from enum import Enum


class DispatcherMode(str, Enum):
    FAST = "fast"
    MEDIUM = "medium"
    SLOW = "slow"
