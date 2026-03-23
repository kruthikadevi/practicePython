# ============================================================
# 07 - Tuples, Sets, and Dictionaries
# ============================================================

# ========================
# TUPLES — immutable list
# ========================
point = (3, 5)
rgb = (255, 128, 0)
single = (42,)       # trailing comma required for single-element tuple!
empty = ()

# Packing & Unpacking
coords = 10, 20      # packing (no parentheses needed)
x, y = coords        # unpacking
print(x, y)

a, *b, c = (1, 2, 3, 4, 5)
print(a, b, c)       # 1 [2,3,4] 5

# Tuple methods (only 2!)
t = (1, 2, 2, 3, 2)
print(t.count(2))    # 3
print(t.index(3))    # 3

# Why use tuples over lists?
# 1. Immutability — safer for constants / config
# 2. Hashable — can be dict keys or set members
# 3. Faster than lists
# 4. Unpacking / multiple return values

def get_dimensions():
    return 1920, 1080   # returns a tuple

width, height = get_dimensions()
print(width, height)

# Named tuples — tuple with field names
from collections import namedtuple

Engineer = namedtuple("Engineer", ["name", "role", "experience"])
eng = Engineer("KruthikaDevi", "Data Engineer", 7)
print(eng.name)
print(eng.role)
print(eng)

# ========================
# SETS — unordered, unique
# ========================
s = {1, 2, 3, 4}
empty_set = set()    # NOT {} — that's an empty dict!

# Adding / removing
s.add(5)
s.add(3)          # duplicate — ignored silently
s.remove(1)       # raises KeyError if not found
s.discard(99)     # safe — no error if not found
popped = s.pop()  # removes arbitrary element

# Set operations
a = {1, 2, 3, 4, 5}
b = {4, 5, 6, 7, 8}

print(a | b)   # Union        {1,2,3,4,5,6,7,8}
print(a & b)   # Intersection {4,5}
print(a - b)   # Difference   {1,2,3}
print(a ^ b)   # Symmetric diff {1,2,3,6,7,8}

# Method equivalents
print(a.union(b))
print(a.intersection(b))
print(a.difference(b))
print(a.symmetric_difference(b))

# Subset / superset
print({1, 2}.issubset(a))    # True
print(a.issuperset({1, 2}))  # True
print(a.isdisjoint({9, 10})) # True — no common elements

# Common use: deduplication
dupes = [1, 2, 2, 3, 3, 3, 4]
unique = list(set(dupes))     # order NOT guaranteed
print(unique)

# Frozenset — immutable set (hashable, can be dict key)
fs = frozenset([1, 2, 3])
print(fs)

# Set comprehension
squares = {x**2 for x in range(1, 6)}
print(squares)

# ========================
# DICTIONARIES — key:value
# ========================
# Creating dicts
empty_dict = {}
person = {"name": "KruthikaDevi", "role": "Data Engineer", "exp": 7}
from_keys = dict.fromkeys(["a", "b", "c"], 0)   # {'a':0,'b':0,'c':0}
zipped = dict(zip(["x", "y"], [10, 20]))         # {'x':10,'y':20}

# Accessing
print(person["name"])           # KeyError if missing
print(person.get("name"))       # None if missing (safe)
print(person.get("age", 0))     # default value if missing

# Modifying
person["company"] = "Cognizant"  # add
person["exp"] = 8                # update
del person["company"]            # delete key
removed = person.pop("exp")      # remove & return value
person.setdefault("city", "Chennai")  # add only if key doesn't exist

# Merging dicts
defaults = {"timeout": 30, "retries": 3}
overrides = {"timeout": 60, "debug": True}

# Python 3.9+
merged = defaults | overrides
print(merged)   # {'timeout': 60, 'retries': 3, 'debug': True}

# older way
merged_old = {**defaults, **overrides}

# Iterating
config = {"host": "localhost", "port": 5432, "db": "analytics"}
for key in config:
    print(key)
for val in config.values():
    print(val)
for k, v in config.items():
    print(f"  {k}: {v}")

# Check membership (checks keys by default)
print("host" in config)         # True
print("localhost" in config.values())  # True

# Dict methods
print(config.keys())
print(config.values())
print(config.items())

copy = config.copy()
config.update({"port": 5433, "schema": "public"})

# ------ Nested Dicts ------
pipeline = {
    "name": "ETL_Daily",
    "source": {"type": "S3", "bucket": "raw-data"},
    "sink": {"type": "Databricks", "table": "silver.orders"},
    "schedule": "0 2 * * *"
}
print(pipeline["source"]["bucket"])   # raw-data
print(pipeline.get("sink", {}).get("table"))  # silver.orders

# ------ Dict Comprehensions ------
squares = {x: x**2 for x in range(1, 6)}
filtered = {k: v for k, v in squares.items() if v > 5}
inverted = {v: k for k, v in squares.items()}
print(squares)
print(filtered)
print(inverted)

# ------ defaultdict & Counter ------
from collections import defaultdict, Counter

word_count = defaultdict(int)
words = "the cat sat on the mat the cat".split()
for word in words:
    word_count[word] += 1   # no KeyError on first access
print(dict(word_count))

counter = Counter(words)
print(counter.most_common(2))   # [('the', 3), ('cat', 2)]

# ------ OrderedDict (3.7+ regular dicts maintain insertion order) ------
from collections import OrderedDict
od = OrderedDict([("a", 1), ("b", 2), ("c", 3)])

# ------ Practice Exercises ------
# 1. Count character frequency in "mississippi" using a dict.
# 2. Find common elements in two lists using sets.
# 3. Invert a dictionary (swap keys and values).
# 4. Group a list of words by their first letter using defaultdict.
# 5. Given a list of dicts (employees), build a new dict of name→salary.
