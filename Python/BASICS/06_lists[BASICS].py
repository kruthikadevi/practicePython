# ============================================================
# 06 - Lists — Creation, Methods, Slicing, Sorting, Tricks
# ============================================================

# ------ Creating Lists ------
empty = []
numbers = [1, 2, 3, 4, 5]
mixed = [1, "hello", 3.14, True, None]
nested = [[1, 2], [3, 4], [5, 6]]
from_range = list(range(1, 11))
from_string = list("Python")   # ['P','y','t','h','o','n']

# ------ Indexing & Slicing ------
tools = ["PySpark", "Databricks", "Airflow", "Hadoop", "Hive", "Terraform"]
print(tools[0])      # PySpark
print(tools[-1])     # Terraform
print(tools[1:4])    # ['Databricks', 'Airflow', 'Hadoop']
print(tools[::-1])   # reversed
print(tools[::2])    # every other element

# ------ Modifying ------
tools[0] = "Apache Spark"   # update
print(tools)

# ------ List Methods ------
stack = []
stack.append("a")       # add to end
stack.append("b")
stack.append("c")
stack.pop()             # remove & return last → "c"
stack.pop(0)            # remove & return at index 0 → "a"
print(stack)            # ['b']

nums = [3, 1, 4, 1, 5, 9, 2, 6]
nums.append(7)          # add single item at end
nums.extend([8, 10])    # add multiple items
nums.insert(0, 0)       # insert at index

print(nums.count(1))    # 2 — how many times 1 appears
print(nums.index(9))    # position of 9

nums.remove(1)          # removes FIRST occurrence of 1
nums.sort()             # sorts in-place ascending
nums.sort(reverse=True) # sort descending
nums.reverse()          # reverses in-place

copy1 = nums.copy()     # shallow copy
copy2 = nums[:]         # also shallow copy
import copy
deep = copy.deepcopy(nested)  # deep copy for nested lists

nums.clear()            # empties list
print(nums)             # []

# ------ Sorting ------
words = ["banana", "Apple", "cherry", "date"]
sorted_words = sorted(words)               # new list, case-sensitive
sorted_lower = sorted(words, key=str.lower)  # case-insensitive
print(sorted_words)
print(sorted_lower)

# Sort by length
sorted_by_len = sorted(words, key=len)
print(sorted_by_len)

# Sort list of dicts
engineers = [
    {"name": "Alice", "exp": 5},
    {"name": "Bob", "exp": 3},
    {"name": "Charlie", "exp": 8},
]
by_exp = sorted(engineers, key=lambda e: e["exp"], reverse=True)
for e in by_exp:
    print(e)

# ------ List Operations ------
a = [1, 2, 3]
b = [4, 5, 6]
print(a + b)        # [1,2,3,4,5,6] — concatenation
print(a * 3)        # [1,2,3,1,2,3,1,2,3]
print(3 in a)       # True
print(len(a))       # 3
print(min(a), max(a), sum(a))  # 1 3 6

# ------ Unpacking ------
first, second, *rest = [10, 20, 30, 40, 50]
print(first, second, rest)   # 10 20 [30, 40, 50]

*head, last = [10, 20, 30, 40]
print(head, last)            # [10, 20, 30] 40

# Swap values
x, y = 1, 2
x, y = y, x
print(x, y)  # 2 1

# ------ 2D Lists / Matrix ------
matrix = [[0] * 3 for _ in range(3)]  # 3x3 zeros
matrix[1][1] = 5
for row in matrix:
    print(row)

# Transpose a matrix
transposed = [[row[i] for row in matrix] for i in range(len(matrix[0]))]
print(transposed)

# ------ Common Pitfall: Shallow Copy ------
original = [[1, 2], [3, 4]]
wrong_copy = original * 2   # ❌ inner lists are shared!
shallow = [row[:] for row in original]  # ✅ proper copy

# ------ Useful Built-ins with Lists ------
nums = [3, 1, 4, 1, 5, 9]
print(list(enumerate(nums)))    # [(0,3),(1,1)...]
print(list(zip(nums, nums)))    # [(3,3),(1,1)...]
print(list(map(str, nums)))     # ['3','1','4'...]
print(list(filter(lambda x: x > 3, nums)))  # [4,5,9]

from functools import reduce
product = reduce(lambda a, b: a * b, nums)
print(product)  # 540

# ------ Practice Exercises ------
# 1. Create a list of 10 random integers and sort them both ascending and descending.
# 2. Remove all duplicates from a list (hint: use set).
# 3. Flatten [[1,2],[3,4],[5,6]] using a list comprehension.
# 4. Find the second largest number in a list without using sort.
# 5. Rotate a list to the right by k positions.
