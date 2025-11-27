from enum import Enum

class Side(Enum):
    buy = auto()
    sell = auto()
    BLUE = auto()

class Limit:
    x: int

