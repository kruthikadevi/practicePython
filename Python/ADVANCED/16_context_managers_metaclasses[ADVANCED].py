# ============================================================
# 02 - Context Managers & Metaclasses
# ============================================================

import time
import threading
from contextlib import contextmanager, asynccontextmanager, suppress, ExitStack

# ============================================================
# CONTEXT MANAGERS
# ============================================================

# ------ Class-based Context Manager ------
class Timer:
    def __init__(self, label=""):
        self.label = label
        self.elapsed = None

    def __enter__(self):
        self._start = time.perf_counter()
        return self   # value bound to 'as' variable

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.elapsed = time.perf_counter() - self._start
        label = f"[{self.label}] " if self.label else ""
        print(f"{label}Elapsed: {self.elapsed:.6f}s")
        return False   # don't suppress exceptions

with Timer("data transform") as t:
    total = sum(range(1_000_000))
print(f"Captured: {t.elapsed:.6f}s")

# ------ Generator-based Context Manager ------
@contextmanager
def managed_connection(host: str, port: int):
    print(f"Opening connection to {host}:{port}")
    conn = {"host": host, "port": port, "open": True}
    try:
        yield conn
    except Exception as e:
        print(f"Error during connection: {e}")
        raise
    finally:
        conn["open"] = False
        print(f"Connection to {host}:{port} closed")

with managed_connection("prod-db", 5432) as conn:
    print(f"Using connection: {conn}")

# ------ suppress — cleaner than try/except for ignoring errors ------
import os
with suppress(FileNotFoundError):
    os.remove("/tmp/nonexistent_file.txt")
print("Continued after suppressed FileNotFoundError")

# ------ ExitStack — dynamic number of context managers ------
files = ["/dev/null", "/dev/null", "/dev/null"]

with ExitStack() as stack:
    handles = [stack.enter_context(open(f)) for f in files]
    print(f"Opened {len(handles)} files")
# All files auto-closed here

# ------ Reentrant Context Manager ------
from contextlib import contextmanager

@contextmanager
def indent_log(level=0):
    prefix = "  " * level
    print(f"{prefix}→ entering block")
    yield prefix
    print(f"{prefix}← exiting block")

with indent_log(0) as p0:
    print(f"{p0}top level")
    with indent_log(1) as p1:
        print(f"{p1}nested")
        with indent_log(2) as p2:
            print(f"{p2}deeply nested")

# ------ Thread Lock as context manager ------
lock = threading.Lock()
shared_data = []

def safe_append(item):
    with lock:
        shared_data.append(item)   # thread-safe

# ------ Practical: temp directory ------
import tempfile, shutil

@contextmanager
def temp_workspace():
    workspace = tempfile.mkdtemp(prefix="pipeline_")
    print(f"Created workspace: {workspace}")
    try:
        yield workspace
    finally:
        shutil.rmtree(workspace)
        print(f"Cleaned up workspace")

with temp_workspace() as ws:
    output_file = os.path.join(ws, "output.csv")
    with open(output_file, "w") as f:
        f.write("id,value\n1,100\n")
    print(f"Files in workspace: {os.listdir(ws)}")

# ============================================================
# METACLASSES
# ============================================================
# A metaclass is the class of a class.
# type is the default metaclass for all classes in Python.

# ------ type() to inspect and create classes dynamically ------
print(type(int))     # <class 'type'>
print(type(str))     # <class 'type'>

class Foo: pass
print(type(Foo))     # <class 'type'>

# Dynamic class creation using type(name, bases, namespace)
Dog = type("Dog", (object,), {
    "sound": "woof",
    "speak": lambda self: f"The dog says {self.sound}",
})
d = Dog()
print(d.speak())

# ------ Custom Metaclass ------
class SingletonMeta(type):
    """Metaclass that enforces Singleton pattern."""
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]

class SparkSession(metaclass=SingletonMeta):
    def __init__(self, app_name):
        self.app_name = app_name
        print(f"SparkSession created: {app_name}")

s1 = SparkSession("ETL_Pipeline")
s2 = SparkSession("Another_App")   # returns existing instance
print(s1 is s2)     # True — same object

# ------ Metaclass for validation ------
class ValidatedMeta(type):
    """Ensures all public methods have docstrings."""
    def __new__(mcs, name, bases, namespace):
        for attr_name, attr_val in namespace.items():
            if callable(attr_val) and not attr_name.startswith("_"):
                if not attr_val.__doc__:
                    raise TypeError(
                        f"Method '{attr_name}' in class '{name}' must have a docstring."
                    )
        return super().__new__(mcs, name, bases, namespace)

try:
    class ETLJob(metaclass=ValidatedMeta):
        def run(self):   # no docstring → TypeError!
            pass
except TypeError as e:
    print(e)

class ETLJobGood(metaclass=ValidatedMeta):
    def run(self):
        """Executes the ETL pipeline."""
        pass

# ------ __init_subclass__ — lighter alternative to metaclasses ------
class Plugin:
    _registry = {}

    def __init_subclass__(cls, plugin_name: str, **kwargs):
        super().__init_subclass__(**kwargs)
        Plugin._registry[plugin_name] = cls
        print(f"Registered plugin: {plugin_name}")

class S3Plugin(Plugin, plugin_name="s3"):
    def read(self): return "reading from S3"

class GCSPlugin(Plugin, plugin_name="gcs"):
    def read(self): return "reading from GCS"

print(Plugin._registry)
source = Plugin._registry["s3"]()
print(source.read())

# ------ Abstract Base Classes with metaclass ------
from abc import ABCMeta, abstractmethod

class DataSource(metaclass=ABCMeta):
    @abstractmethod
    def connect(self): pass

    @abstractmethod
    def read(self, query: str) -> list: pass

    @classmethod
    def __subclasshook__(cls, C):
        # Customize isinstance() / issubclass() behavior
        if cls is DataSource:
            if any("connect" in B.__dict__ and "read" in B.__dict__
                   for B in C.__mro__):
                return True
        return NotImplemented

# ------ __class_getitem__ — for generic types ------
class TypedList:
    def __class_getitem__(cls, item_type):
        class _TypedList(cls):
            _type = item_type
            def append(self, item):
                if not isinstance(item, self._type):
                    raise TypeError(f"Expected {self._type}, got {type(item)}")
                super().append(item)
        _TypedList.__name__ = f"TypedList[{item_type.__name__}]"
        return _TypedList

IntList = TypedList[int]
lst = IntList()
lst.append(1)
lst.append(2)
try:
    lst.append("not an int")
except TypeError as e:
    print(e)

# ------ Practice Exercises ------
# 1. Write a context manager that redirects stdout to a file.
# 2. Create a metaclass that automatically adds a __repr__ to every class.
# 3. Build a plugin registry using __init_subclass__ for data sources.
# 4. Write a context manager that rolls back a list if an exception occurs.
# 5. Create a metaclass that logs every method call on a class.
