# ============================================================
# 04 - Type Hints, Protocols, TypeVar, Generic Classes
# ============================================================

from __future__ import annotations   # PEP 563 — deferred evaluation
from typing import (
    List, Dict, Tuple, Set, Optional, Union, Any, Callable,
    TypeVar, Generic, Protocol, runtime_checkable,
    Literal, Final, ClassVar, TypedDict, NamedTuple,
    overload, TYPE_CHECKING
)
from dataclasses import dataclass, field
import abc

# ------ Basic Type Hints ------
def greet(name: str, times: int = 1) -> str:
    return (f"Hello, {name}! " * times).strip()

# ------ Collection Types (Python 3.9+ use built-ins directly) ------
def process(items: list[int]) -> dict[str, int]:
    return {"sum": sum(items), "count": len(items)}

# Older style (3.8 and below needs from typing import List, Dict)
def process_old(items: List[int]) -> Dict[str, int]:
    return {"sum": sum(items), "count": len(items)}

# ------ Optional / Union ------
def find_user(user_id: int) -> Optional[dict]:   # Optional[X] == Union[X, None]
    users = {1: {"name": "Kruthika"}}
    return users.get(user_id)

def parse_value(value: Union[str, int, float]) -> float:
    return float(value)

# Python 3.10+ shorthand
def parse_value_new(value: str | int | float) -> float:
    return float(value)

# ------ Callable ------
from typing import Callable

Transform = Callable[[list[dict]], list[dict]]

def apply_transform(data: list[dict], fn: Transform) -> list[dict]:
    return fn(data)

def uppercase_names(records: list[dict]) -> list[dict]:
    return [{**r, "name": r["name"].upper()} for r in records]

records = [{"name": "kruthika", "exp": 7}]
print(apply_transform(records, uppercase_names))

# ------ Literal ------
from typing import Literal

LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
Environment = Literal["dev", "staging", "prod"]

def configure_logger(level: LogLevel, env: Environment) -> None:
    print(f"Logger: level={level}, env={env}")

configure_logger("INFO", "prod")

# ------ Final ------
MAX_RETRIES: Final = 3
PI: Final[float] = 3.14159

# ------ TypedDict ------
class PipelineConfig(TypedDict):
    name: str
    source: str
    sink: str
    batch_size: int
    active: bool

class ExtendedConfig(PipelineConfig, total=False):   # total=False → optional fields
    debug: bool
    max_records: int

config: PipelineConfig = {
    "name": "ETL_Daily",
    "source": "s3://bucket/raw",
    "sink": "databricks.silver.orders",
    "batch_size": 1000,
    "active": True
}

# ------ NamedTuple ------
class Coordinate(NamedTuple):
    x: float
    y: float
    z: float = 0.0

    def distance_from_origin(self) -> float:
        return (self.x**2 + self.y**2 + self.z**2) ** 0.5

c = Coordinate(3.0, 4.0)
print(c.distance_from_origin())   # 5.0
print(c.x, c.y, c.z)

# ------ TypeVar — Generic functions ------
T = TypeVar("T")
K = TypeVar("K")
V = TypeVar("V")
NumT = TypeVar("NumT", int, float)   # constrained TypeVar

def first(items: list[T]) -> T:
    return items[0]

def last(items: list[T]) -> T:
    return items[-1]

print(first([1, 2, 3]))        # 1
print(first(["a", "b", "c"]))  # "a"

def clamp(value: NumT, lo: NumT, hi: NumT) -> NumT:
    return max(lo, min(hi, value))

print(clamp(10, 0, 5))    # 5
print(clamp(3.5, 0.0, 5.0)) # 3.5

# ------ Generic Classes ------
class Stack(Generic[T]):
    def __init__(self) -> None:
        self._items: list[T] = []

    def push(self, item: T) -> None:
        self._items.append(item)

    def pop(self) -> T:
        return self._items.pop()

    def peek(self) -> T:
        return self._items[-1]

    def is_empty(self) -> bool:
        return not self._items

    def __len__(self) -> int:
        return len(self._items)

int_stack: Stack[int] = Stack()
int_stack.push(1)
int_stack.push(2)
print(int_stack.pop())   # 2

str_stack: Stack[str] = Stack()
str_stack.push("ETL")
str_stack.push("ELT")
print(str_stack.peek())

# ------ Protocol — Structural Subtyping (Duck Typing + Types) ------
@runtime_checkable
class Readable(Protocol):
    def read(self, n: int = -1) -> str: ...

@runtime_checkable
class DataSource(Protocol):
    def connect(self) -> None: ...
    def fetch(self, query: str) -> list[dict]: ...
    def close(self) -> None: ...

class S3Source:
    def connect(self) -> None:
        print("Connected to S3")

    def fetch(self, query: str) -> list[dict]:
        return [{"id": 1, "data": query}]

    def close(self) -> None:
        print("Disconnected from S3")

class BigQuerySource:
    def connect(self) -> None:
        print("Connected to BigQuery")

    def fetch(self, query: str) -> list[dict]:
        return [{"row": 1, "query": query}]

    def close(self) -> None:
        print("BigQuery connection closed")

def run_pipeline(source: DataSource, query: str) -> list[dict]:
    source.connect()
    try:
        return source.fetch(query)
    finally:
        source.close()

# Both work — no inheritance required (structural subtyping)
for src_class in [S3Source, BigQuerySource]:
    src = src_class()
    result = run_pipeline(src, "SELECT * FROM orders")
    print(result)

print(isinstance(S3Source(), DataSource))  # True — runtime check

# ------ overload — multiple signatures ------
@overload
def process(x: int) -> int: ...
@overload
def process(x: str) -> str: ...
@overload
def process(x: list) -> list: ...

def process(x):
    if isinstance(x, int):
        return x * 2
    elif isinstance(x, str):
        return x.upper()
    elif isinstance(x, list):
        return [process(i) for i in x]

print(process(5))         # 10
print(process("hello"))   # HELLO
print(process([1, 2, 3])) # [2, 4, 6]

# ------ ClassVar ------
class PipelineRegistry:
    _registry: ClassVar[dict[str, type]] = {}
    instance_id: int

    def __init__(self, iid: int):
        self.instance_id = iid

    @classmethod
    def register(cls, name: str, pipeline_cls: type) -> None:
        cls._registry[name] = pipeline_cls

# ------ Practice Exercises ------
# 1. Define a TypedDict for a Spark job configuration.
# 2. Create a Generic Repository[T] class with add, get, list methods.
# 3. Define a Protocol for a Transformer that has fit() and transform().
# 4. Use overload to type a function that accepts int or str and returns different types.
# 5. Annotate a full data pipeline function with all proper type hints.
