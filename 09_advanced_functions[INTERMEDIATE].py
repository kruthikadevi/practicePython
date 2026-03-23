# ============================================================
# 01 - Advanced Functions — Lambda, Closures, Higher-Order
# ============================================================

# ------ Lambda Functions ------
# Anonymous, single-expression functions
square = lambda x: x ** 2
add = lambda x, y: x + y
greet = lambda name: f"Hello, {name}!"

print(square(5))        # 25
print(add(3, 4))        # 7

# Best use: short callbacks — don't assign lambdas to variables (use def)
numbers = [5, 2, 8, 1, 9, 3]
sorted_nums = sorted(numbers, key=lambda x: -x)   # descending
print(sorted_nums)

employees = [
    {"name": "Alice", "salary": 80000, "exp": 5},
    {"name": "Bob",   "salary": 95000, "exp": 8},
    {"name": "Carol", "salary": 72000, "exp": 3},
]
by_salary = sorted(employees, key=lambda e: e["salary"], reverse=True)
for e in by_salary:
    print(e)

# ------ map() / filter() / reduce() ------
nums = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

# map — apply function to every element
squares = list(map(lambda x: x**2, nums))
strings = list(map(str, nums))
print(squares)

# filter — keep elements where function returns True
evens = list(filter(lambda x: x % 2 == 0, nums))
print(evens)

# reduce — fold left
from functools import reduce
product = reduce(lambda a, b: a * b, nums)    # 10!
print(product)   # 3628800

# Modern Pythonic equivalents (usually preferred over map/filter)
squares_comp = [x**2 for x in nums]          # list comp instead of map
evens_comp   = [x for x in nums if x % 2 == 0]  # list comp instead of filter

# ------ Higher-Order Functions ------
def apply_twice(func, value):
    return func(func(value))

print(apply_twice(lambda x: x * 2, 3))   # 12

def compose(*funcs):
    """Compose functions right-to-left: compose(f, g)(x) = f(g(x))"""
    def composed(x):
        for f in reversed(funcs):
            x = f(x)
        return x
    return composed

double = lambda x: x * 2
add_ten = lambda x: x + 10
double_then_add = compose(add_ten, double)
print(double_then_add(5))   # (5*2)+10 = 20

# ------ Closures ------
# A closure is a function that remembers variables from its enclosing scope
# even after the enclosing function has finished executing.

def make_multiplier(factor):
    def multiplier(x):
        return x * factor   # 'factor' is captured from outer scope
    return multiplier

double = make_multiplier(2)
triple = make_multiplier(3)

print(double(5))    # 10
print(triple(5))    # 15
print(double.__closure__[0].cell_contents)  # 2 — inspect captured var

# Real-world: configurable logger
def make_logger(prefix, level="INFO"):
    def log(message):
        print(f"[{level}] {prefix}: {message}")
    return log

pipeline_log = make_logger("ETL_Pipeline")
error_log    = make_logger("ETL_Pipeline", level="ERROR")

pipeline_log("Started reading from S3")
error_log("Failed to connect to sink")

# ------ Partial Functions ------
from functools import partial

def power(base, exponent):
    return base ** exponent

square = partial(power, exponent=2)
cube   = partial(power, exponent=3)

print(square(5))   # 25
print(cube(3))     # 27

# Practical: pre-fill common args
import json
pretty_print = partial(json.dumps, indent=4, sort_keys=True)
data = {"z": 3, "a": 1, "m": 2}
print(pretty_print(data))

# ------ Memoization / Caching ------
from functools import lru_cache

@lru_cache(maxsize=None)
def fibonacci(n):
    if n < 2:
        return n
    return fibonacci(n-1) + fibonacci(n-2)

print(fibonacci(50))   # instant — cached
print(fibonacci.cache_info())   # hits, misses, maxsize

# Manual memoization
def memoize(func):
    cache = {}
    def wrapper(*args):
        if args not in cache:
            cache[args] = func(*args)
        return cache[args]
    return wrapper

@memoize
def slow_square(n):
    return n * n

print(slow_square(10))
print(slow_square(10))   # from cache

# ------ Practice Exercises ------
# 1. Use map() and lambda to convert a list of temps from Celsius to Fahrenheit.
# 2. Use filter() to get only engineers with > 5 years from a list of dicts.
# 3. Write a closure that generates unique sequential IDs.
# 4. Use partial to create a 'log_error' function pre-filled with level="ERROR".
# 5. Implement a decorator (hint: next file!) to time a function's execution.
