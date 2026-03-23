# ============================================================
# 01 - Decorators — Functions, Classes, Stacking, Parametrized
# ============================================================

import time
import functools
import logging

logging.basicConfig(level=logging.INFO)

# ------ What is a decorator? ------
# A decorator is a function that takes a function and returns a new function.
# It lets you add behavior before/after a function without modifying it.

# Manual version (what the @ syntax does):
def my_decorator(func):
    def wrapper(*args, **kwargs):
        print("Before function")
        result = func(*args, **kwargs)
        print("After function")
        return result
    return wrapper

def say_hello():
    print("Hello!")

say_hello = my_decorator(say_hello)   # equivalent to @my_decorator
say_hello()

# ------ Using @ syntax ------
@my_decorator
def say_world():
    print("World!")

say_world()

# ------ @functools.wraps — preserve metadata ------
def timer(func):
    @functools.wraps(func)   # copies __name__, __doc__, etc.
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = func(*args, **kwargs)
        elapsed = time.perf_counter() - start
        print(f"[TIMER] {func.__name__} took {elapsed:.6f}s")
        return result
    return wrapper

@timer
def slow_sum(n: int) -> int:
    """Returns sum of 0..n."""
    return sum(range(n))

print(slow_sum(1_000_000))
print(slow_sum.__name__)   # "slow_sum" (not "wrapper")
print(slow_sum.__doc__)    # "Returns sum of 0..n."

# ------ Practical decorators ------

# 1. Logger
def log_call(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logging.info(f"Calling {func.__name__}({args}, {kwargs})")
        try:
            result = func(*args, **kwargs)
            logging.info(f"{func.__name__} returned {result!r}")
            return result
        except Exception as e:
            logging.error(f"{func.__name__} raised {type(e).__name__}: {e}")
            raise
    return wrapper

@log_call
def divide(a, b):
    return a / b

divide(10, 2)

# 2. Retry
def retry(max_attempts=3, delay=1.0, exceptions=(Exception,)):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_attempts:
                        raise
                    print(f"[RETRY] {func.__name__} attempt {attempt} failed: {e}. Retrying...")
                    time.sleep(delay)
        return wrapper
    return decorator

import random
@retry(max_attempts=5, delay=0.1, exceptions=(ConnectionError,))
def fetch_data():
    if random.random() < 0.6:
        raise ConnectionError("Network error")
    return {"data": [1, 2, 3]}

try:
    print(fetch_data())
except ConnectionError:
    print("All retries failed")

# 3. Cache (simple custom version)
def simple_cache(func):
    cache = {}
    @functools.wraps(func)
    def wrapper(*args):
        if args not in cache:
            cache[args] = func(*args)
        return cache[args]
    wrapper.cache = cache
    wrapper.clear_cache = lambda: cache.clear()
    return wrapper

@simple_cache
def fib(n):
    if n < 2: return n
    return fib(n - 1) + fib(n - 2)

print(fib(40))
print(f"Cache size: {len(fib.cache)}")

# 4. Validate types
def validate_types(**type_map):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            import inspect
            sig = inspect.signature(func)
            bound = sig.bind(*args, **kwargs)
            bound.apply_defaults()
            for param_name, expected_type in type_map.items():
                if param_name in bound.arguments:
                    val = bound.arguments[param_name]
                    if not isinstance(val, expected_type):
                        raise TypeError(
                            f"Parameter '{param_name}' expected {expected_type.__name__}, "
                            f"got {type(val).__name__}"
                        )
            return func(*args, **kwargs)
        return wrapper
    return decorator

@validate_types(name=str, experience=int)
def create_engineer(name: str, experience: int) -> dict:
    return {"name": name, "experience": experience}

print(create_engineer("Kruthika", 7))
try:
    create_engineer("Kruthika", "seven")
except TypeError as e:
    print(e)

# ------ Stacking Decorators ------
# Applied bottom-up: @A @B def f → A(B(f))

@timer
@log_call
@retry(max_attempts=2, delay=0.0)
def process_pipeline(name: str) -> str:
    return f"Pipeline {name} complete"

process_pipeline("ETL_Daily")

# ------ Class-based Decorators ------
class CountCalls:
    """Counts how many times a function has been called."""
    def __init__(self, func):
        functools.update_wrapper(self, func)
        self.func = func
        self.call_count = 0

    def __call__(self, *args, **kwargs):
        self.call_count += 1
        print(f"[{self.func.__name__}] call #{self.call_count}")
        return self.func(*args, **kwargs)

@CountCalls
def run_job(job_name):
    print(f"Running: {job_name}")

run_job("ingest")
run_job("transform")
run_job("load")
print(f"Total calls: {run_job.call_count}")

# ------ Decorator with optional arguments ------
def repeat(_func=None, *, times=1):
    """Works with or without arguments: @repeat or @repeat(times=3)"""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for _ in range(times):
                result = func(*args, **kwargs)
            return result
        return wrapper

    if _func is None:
        return decorator     # called with args: @repeat(times=3)
    return decorator(_func)  # called without args: @repeat

@repeat
def say_hi(): print("Hi!")

@repeat(times=3)
def say_bye(): print("Bye!")

say_hi()
say_bye()

# ------ Property decorator (built-in) ------
class Temperature:
    def __init__(self, celsius=0):
        self._celsius = celsius

    @property
    def celsius(self): return self._celsius

    @celsius.setter
    def celsius(self, val):
        if val < -273.15:
            raise ValueError("Below absolute zero!")
        self._celsius = val

    @property
    def fahrenheit(self):
        return self._celsius * 9/5 + 32

t = Temperature(100)
print(t.fahrenheit)   # 212.0
t.celsius = 0
print(t.fahrenheit)   # 32.0

# ------ Practice Exercises ------
# 1. Write a @debug decorator that prints all args/kwargs and return value.
# 2. Create a @singleton decorator that ensures only one instance of a class.
# 3. Write a @deprecated decorator that logs a warning when called.
# 4. Build a @rate_limit(calls=5, period=60) decorator.
# 5. Combine @timer + @retry into one @resilient_timer decorator.
