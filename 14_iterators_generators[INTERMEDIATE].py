# ============================================================
# 06 - Iterators and Generators
# ============================================================

import sys

# ------ Iterator Protocol ------
# An object is iterable if it implements __iter__()
# An iterator implements __iter__() AND __next__()

class CountUp:
    """Custom iterator that counts from start to end."""
    def __init__(self, start, end):
        self.current = start
        self.end = end

    def __iter__(self):
        return self   # iterator returns itself

    def __next__(self):
        if self.current > self.end:
            raise StopIteration
        val = self.current
        self.current += 1
        return val

counter = CountUp(1, 5)
for n in counter:
    print(n, end=" ")   # 1 2 3 4 5
print()

# Manual iteration
it = iter([10, 20, 30])
print(next(it))   # 10
print(next(it))   # 20
print(next(it))   # 30
# next(it)        # ← StopIteration

# Default sentinel
result = next(iter([]), "empty")   # no exception
print(result)  # "empty"

# ------ Generators — lazy iterators using yield ------
def count_up_gen(start, end):
    """Generator version — no class needed."""
    current = start
    while current <= end:
        yield current
        current += 1

gen = count_up_gen(1, 5)
print(list(gen))         # [1, 2, 3, 4, 5]

# Generators are lazy — values computed on demand
def infinite_ids(prefix="job"):
    n = 1
    while True:
        yield f"{prefix}_{n:04d}"
        n += 1

id_gen = infinite_ids("pipeline")
print(next(id_gen))    # pipeline_0001
print(next(id_gen))    # pipeline_0002
print(next(id_gen))    # pipeline_0003

# ------ Memory comparison ------
def list_squares(n):
    return [x**2 for x in range(n)]   # stores all in memory

def gen_squares(n):
    for x in range(n):                # yields one at a time
        yield x**2

n = 1_000_000
list_size = sys.getsizeof(list_squares(n))
gen_obj   = gen_squares(n)
gen_size  = sys.getsizeof(gen_obj)

print(f"List: {list_size:,} bytes")   # ~8 MB
print(f"Gen:  {gen_size} bytes")       # ~112 bytes!

# ------ Generator Pipelines ------
# Process data in stages without materializing intermediate results

def read_lines(path):
    """Stage 1: yield lines from file."""
    with open(path) as f:
        yield from f

def parse_csv_lines(lines):
    """Stage 2: split each line."""
    import csv
    reader = csv.reader(lines)
    yield from reader

def filter_rows(rows, column_idx, value):
    """Stage 3: filter."""
    for row in rows:
        if len(row) > column_idx and row[column_idx] == value:
            yield row

# Pipeline (nothing executes until we consume)
# lines = read_lines("big.csv")
# parsed = parse_csv_lines(lines)
# filtered = filter_rows(parsed, 0, "DE")
# for row in filtered:
#     process(row)

# ------ yield from ------
def flatten(nested):
    for item in nested:
        if isinstance(item, (list, tuple)):
            yield from flatten(item)   # delegate to sub-generator
        else:
            yield item

nested = [1, [2, 3, [4, 5]], 6, [7, [8, [9]]]]
print(list(flatten(nested)))   # [1, 2, 3, 4, 5, 6, 7, 8, 9]

# ------ Generator Expressions ------
# Like list comps but lazy
squares_gen = (x**2 for x in range(10))   # () not []
print(sum(x**2 for x in range(1001)))     # no intermediate list!

# Chaining generator expressions
data = range(1, 101)
pipeline = (
    x * 2
    for x in data
    if x % 3 == 0
)
print(list(pipeline))

# ------ send() and two-way generators ------
def accumulator():
    total = 0
    while True:
        value = yield total   # yield sends current total, receives next value
        if value is None:
            break
        total += value

acc = accumulator()
next(acc)          # prime the generator (advance to first yield)
print(acc.send(10))   # 10
print(acc.send(20))   # 30
print(acc.send(5))    # 35

# ------ throw() and close() ------
def resilient_gen():
    try:
        while True:
            try:
                val = yield
                print(f"Processing: {val}")
            except ValueError as e:
                print(f"Skipping bad value: {e}")
    except GeneratorExit:
        print("Generator closed cleanly")

g = resilient_gen()
next(g)
g.send("good value")
g.throw(ValueError, "bad data")
g.close()

# ------ Real-world: chunked data processing ------
def chunked(iterable, size):
    """Yield successive chunks of given size from iterable."""
    chunk = []
    for item in iterable:
        chunk.append(item)
        if len(chunk) == size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk   # last partial chunk

data = list(range(1, 22))
for batch in chunked(data, 5):
    print(f"Batch of {len(batch)}: {batch}")

# ------ itertools generators ------
from itertools import islice, takewhile, dropwhile, count, cycle, repeat

# islice — lazy slice
first_10 = list(islice(count(1), 10))   # [1..10]
print(first_10)

# takewhile / dropwhile
nums = [1, 4, 6, 2, 9, 1, 2]
print(list(takewhile(lambda x: x < 7, nums)))  # [1, 4, 6, 2]
print(list(dropwhile(lambda x: x < 7, nums)))  # [9, 1, 2]

# cycle & repeat
import itertools
colors = list(itertools.islice(cycle(["red", "green", "blue"]), 7))
print(colors)

# ------ Practice Exercises ------
# 1. Build a generator that yields Fibonacci numbers indefinitely.
# 2. Create a generator pipeline: read → filter → transform → write.
# 3. Implement a chunked file reader that yields N lines at a time.
# 4. Build an infinite counter generator that resets when it hits a limit.
# 5. Use send() to build a running average generator.
