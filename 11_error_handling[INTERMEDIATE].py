# ============================================================
# 03 - Error Handling — try/except/finally, Custom Exceptions
# ============================================================

import logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# ------ Basic try/except ------
try:
    result = 10 / 0
except ZeroDivisionError as e:
    print(f"Caught: {e}")

# ------ Multiple except clauses ------
def parse_input(value):
    try:
        num = int(value)
        result = 100 / num
        return result
    except ValueError:
        print(f"'{value}' is not a valid integer")
    except ZeroDivisionError:
        print("Cannot divide by zero")
    except (TypeError, AttributeError) as e:
        print(f"Type error: {e}")
    except Exception as e:
        print(f"Unexpected error: {type(e).__name__}: {e}")
    finally:
        print("Always runs — good for cleanup")

parse_input("abc")
parse_input("0")
parse_input("5")

# ------ else clause ------
def read_file(path):
    try:
        with open(path, "r") as f:
            content = f.read()
    except FileNotFoundError:
        print(f"File not found: {path}")
    except PermissionError:
        print(f"No permission to read: {path}")
    else:
        # Only runs if NO exception was raised
        print(f"Successfully read {len(content)} bytes")
        return content
    finally:
        print("read_file() finished")

read_file("nonexistent.txt")

# ------ Exception Hierarchy ------
# BaseException
#   ├── SystemExit
#   ├── KeyboardInterrupt
#   └── Exception
#         ├── ValueError
#         ├── TypeError
#         ├── RuntimeError
#         ├── OSError
#         │     ├── FileNotFoundError
#         │     ├── PermissionError
#         │     └── TimeoutError
#         ├── IndexError
#         ├── KeyError
#         └── StopIteration

# ------ Re-raising exceptions ------
def load_config(path):
    try:
        with open(path) as f:
            import json
            return json.load(f)
    except FileNotFoundError as e:
        logging.error(f"Config file missing: {path}")
        raise   # re-raise same exception

    except Exception as e:
        logging.error(f"Unexpected error loading config")
        raise RuntimeError(f"Config load failed: {e}") from e  # chain exceptions

# ------ Custom Exceptions ------
class DataEngineeringError(Exception):
    """Base class for all pipeline exceptions."""
    pass

class PipelineConfigError(DataEngineeringError):
    """Raised when pipeline configuration is invalid."""
    def __init__(self, field: str, reason: str):
        self.field = field
        self.reason = reason
        super().__init__(f"Config error on '{field}': {reason}")

class DataQualityError(DataEngineeringError):
    """Raised when data quality checks fail."""
    def __init__(self, table: str, failed_checks: list):
        self.table = table
        self.failed_checks = failed_checks
        msg = f"Data quality failed on '{table}': {', '.join(failed_checks)}"
        super().__init__(msg)

class SourceUnavailableError(DataEngineeringError):
    """Raised when a data source cannot be reached."""
    def __init__(self, source: str, details: str = ""):
        self.source = source
        super().__init__(f"Source '{source}' is unavailable. {details}".strip())


# Using custom exceptions
def validate_pipeline_config(config: dict):
    if "source" not in config:
        raise PipelineConfigError("source", "field is required")
    if not isinstance(config.get("batch_size", 1), int):
        raise PipelineConfigError("batch_size", "must be an integer")

def run_dq_checks(table: str, data: list) -> None:
    failed = []
    if not data:
        failed.append("no_data")
    if any(r.get("amount") is None for r in data):
        failed.append("null_amount")
    if failed:
        raise DataQualityError(table, failed)

try:
    validate_pipeline_config({"batch_size": "large"})
except PipelineConfigError as e:
    print(f"Config error: {e.field} — {e.reason}")

try:
    run_dq_checks("orders", [{"id": 1, "amount": None}])
except DataQualityError as e:
    print(f"DQ failed on {e.table}: {e.failed_checks}")

# ------ Context Managers (with statement) ------
# Built-in: files auto-close
with open("/dev/null", "w") as f:
    f.write("test")
# f is closed here even if an exception occurs

# Multiple context managers
# with open("a.txt") as a, open("b.txt") as b:
#     ...

# ------ Custom Context Manager with class ------
class DatabaseConnection:
    def __init__(self, host):
        self.host = host

    def __enter__(self):
        print(f"Connecting to {self.host}...")
        return self   # value bound to 'as' variable

    def __exit__(self, exc_type, exc_val, exc_tb):
        print("Closing connection")
        if exc_type:
            print(f"Exception during context: {exc_type.__name__}: {exc_val}")
        return False   # False = don't suppress exceptions; True = suppress

with DatabaseConnection("prod-db") as conn:
    print(f"Running query on {conn.host}")

# ------ Custom Context Manager with contextlib ------
from contextlib import contextmanager, suppress

@contextmanager
def timer(label=""):
    import time
    start = time.time()
    try:
        yield
    finally:
        elapsed = time.time() - start
        print(f"[{label or 'timer'}] {elapsed:.4f}s")

with timer("data processing"):
    total = sum(range(1_000_000))

# suppress — silently ignore specific exceptions
with suppress(FileNotFoundError):
    import os
    os.remove("ghost_file.txt")   # no error even if file doesn't exist

# ------ Retries Pattern ------
import time
import random

def with_retry(func, retries=3, delay=1.0, exceptions=(Exception,)):
    for attempt in range(1, retries + 1):
        try:
            return func()
        except exceptions as e:
            if attempt == retries:
                raise
            print(f"Attempt {attempt} failed: {e}. Retrying in {delay}s...")
            time.sleep(delay)

def flaky_operation():
    if random.random() < 0.7:
        raise ConnectionError("Transient network error")
    return "success"

try:
    result = with_retry(flaky_operation, retries=5, delay=0.1,
                        exceptions=(ConnectionError,))
    print(result)
except ConnectionError:
    print("All retries exhausted")

# ------ Practice Exercises ------
# 1. Write a safe division function that handles all edge cases.
# 2. Create a custom exception hierarchy for an API client.
# 3. Write a context manager that temporarily changes the working directory.
# 4. Implement a retry decorator (use @functools.wraps).
# 5. Parse a JSON file and raise meaningful custom exceptions for each failure mode.
