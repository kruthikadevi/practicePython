# ============================================================
# 08 - Functions — Basics
# ============================================================
# Topics: def, parameters, return, *args, **kwargs, scope,
#         default args, type hints, docstrings
# ============================================================

# ------ Basic Function ------
def greet(name):
    return f"Hello, {name}!"

print(greet("KruthikaDevi"))

# ------ Multiple Return Values (tuple) ------
def min_max(numbers):
    return min(numbers), max(numbers)

low, high = min_max([3, 1, 4, 1, 5, 9])
print(low, high)   # 1 9

# ------ Default Parameters ------
def connect(host="localhost", port=5432, db="default"):
    return f"Connecting to {db} at {host}:{port}"

print(connect())
print(connect("prod-server", db="analytics"))   # keyword arg skips port

# GOTCHA: Mutable default arguments — use None instead of []
def add_item(item, lst=None):   # ✅ correct
    if lst is None:
        lst = []
    lst.append(item)
    return lst

# def add_item_bad(item, lst=[]):   # ❌ lst is shared across all calls!

# ------ Positional vs Keyword Arguments ------
def describe(name, role, experience):
    print(f"{name} is a {role} with {experience} years of exp.")

describe("Kruthika", "Data Engineer", 7)           # positional
describe(role="Data Engineer", name="Kruthika", experience=7)  # keyword
describe("Kruthika", experience=7, role="Data Engineer")       # mixed

# ------ *args — Variable Positional Arguments ------
def total(*args):
    return sum(args)

print(total(1, 2, 3))           # 6
print(total(10, 20, 30, 40))    # 100

def log_event(event_type, *messages):
    for msg in messages:
        print(f"[{event_type}] {msg}")

log_event("INFO", "Pipeline started", "Reading from S3", "Done")

# ------ **kwargs — Variable Keyword Arguments ------
def create_config(**kwargs):
    for key, val in kwargs.items():
        print(f"  {key}: {val}")

create_config(host="localhost", port=5432, schema="public")

# ------ Combining all parameter types ------
# Order: positional, *args, keyword-only, **kwargs
def full_function(pos1, pos2, *args, kw_only=True, **kwargs):
    print(f"pos: {pos1}, {pos2}")
    print(f"args: {args}")
    print(f"kw_only: {kw_only}")
    print(f"kwargs: {kwargs}")

full_function(1, 2, 3, 4, 5, kw_only=False, debug=True, verbose=False)

# / and * in signatures (Python 3.8+)
def strict(pos_only, /, both, *, kw_only):
    """
    pos_only  — positional only (before /)
    both      — can be positional OR keyword
    kw_only   — keyword only (after *)
    """
    print(pos_only, both, kw_only)

strict(1, 2, kw_only=3)
strict(1, both=2, kw_only=3)

# ------ Scope — LEGB Rule ------
# Local → Enclosing → Global → Built-in
x = "global"

def outer():
    x = "enclosing"
    def inner():
        x = "local"
        print(x)   # local
    inner()
    print(x)       # enclosing

outer()
print(x)           # global

# global keyword
counter = 0
def increment():
    global counter
    counter += 1

increment()
increment()
print(counter)   # 2

# nonlocal keyword
def make_counter():
    count = 0
    def increment():
        nonlocal count
        count += 1
        return count
    return increment

c = make_counter()
print(c())   # 1
print(c())   # 2
print(c())   # 3

# ------ Type Hints & Docstrings ------
def calculate_pipeline_cost(
    gb_processed: float,
    price_per_gb: float = 0.023,
    discount: float = 0.0
) -> float:
    """
    Calculate the cost of a data pipeline run.

    Args:
        gb_processed: Amount of data processed in GB.
        price_per_gb: Cost per GB (default: $0.023 for S3 standard).
        discount: Fractional discount (0.0 to 1.0).

    Returns:
        Total cost in USD.

    Examples:
        >>> calculate_pipeline_cost(100)
        2.3
        >>> calculate_pipeline_cost(1000, discount=0.1)
        20.7
    """
    cost = gb_processed * price_per_gb
    return cost * (1 - discount)

print(calculate_pipeline_cost(500))
print(calculate_pipeline_cost(1000, discount=0.1))
help(calculate_pipeline_cost)   # prints docstring

# ------ Functions as First-Class Objects ------
def square(x): return x ** 2
def cube(x): return x ** 3

operations = [square, cube]
for op in operations:
    print(op(3))   # 9, 27

def apply(func, value):
    return func(value)

print(apply(square, 5))   # 25

# ------ Practice Exercises ------
# 1. Write a function that accepts any number of strings and returns the longest.
# 2. Write a function with **kwargs to build a SQL SELECT statement dynamically.
# 3. Demonstrate the mutable default argument bug and fix it.
# 4. Write a function with type hints that validates an email address.
# 5. Create a counter using closures (nonlocal) that supports reset().
