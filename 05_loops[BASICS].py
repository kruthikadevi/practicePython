# ============================================================
# 05 - Loops — for, while, break, continue, else, comprehensions
# ============================================================

# ------ for loop ------
fruits = ["apple", "banana", "cherry"]
for fruit in fruits:
    print(fruit)

# ------ range() ------
for i in range(5):          # 0,1,2,3,4
    print(i, end=" ")
print()

for i in range(1, 6):       # 1,2,3,4,5
    print(i, end=" ")
print()

for i in range(0, 10, 2):   # 0,2,4,6,8 — step
    print(i, end=" ")
print()

for i in range(10, 0, -1):  # 10,9,...,1 — countdown
    print(i, end=" ")
print()

# ------ enumerate() — index + value ------
tools = ["PySpark", "Databricks", "Airflow", "Terraform"]
for idx, tool in enumerate(tools):
    print(f"{idx + 1}. {tool}")

# ------ zip() — iterate multiple lists together ------
names = ["Alice", "Bob", "Charlie"]
scores = [85, 92, 78]
for name, score in zip(names, scores):
    print(f"{name}: {score}")

# ------ Iterating dict ------
config = {"host": "localhost", "port": 5432, "db": "analytics"}
for key in config:              # keys
    print(key)
for val in config.values():    # values
    print(val)
for k, v in config.items():    # key-value pairs
    print(f"{k} = {v}")

# ------ while loop ------
count = 0
while count < 5:
    print(f"Count: {count}")
    count += 1

# while with user input simulation
attempts = 0
max_attempts = 3
password = "secret"
guesses = ["wrong", "wrong", "secret"]  # simulating input

while attempts < max_attempts:
    guess = guesses[attempts]
    attempts += 1
    if guess == password:
        print("Access granted!")
        break
    print(f"Wrong password. {max_attempts - attempts} attempt(s) left.")
else:
    print("Account locked.")

# ------ break, continue, pass ------
# break — exit loop immediately
for n in range(10):
    if n == 5:
        break
    print(n, end=" ")
print()  # 0 1 2 3 4

# continue — skip current iteration
for n in range(10):
    if n % 2 == 0:
        continue
    print(n, end=" ")   # 1 3 5 7 9
print()

# for...else — else runs only if loop wasn't broken
def find_prime(n):
    for i in range(2, n):
        if n % i == 0:
            print(f"{n} is not prime")
            break
    else:
        print(f"{n} is prime")

find_prime(7)   # prime
find_prime(10)  # not prime

# ------ Nested loops ------
for i in range(1, 4):
    for j in range(1, 4):
        print(f"{i}x{j}={i*j}", end="  ")
    print()

# Flattening nested list
matrix = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
flat = []
for row in matrix:
    for val in row:
        flat.append(val)
print(flat)

# ------ List Comprehensions ------
# [expression for item in iterable if condition]
squares = [x**2 for x in range(1, 11)]
print(squares)

evens = [x for x in range(20) if x % 2 == 0]
print(evens)

upper_tools = [t.upper() for t in tools]
print(upper_tools)

# Nested comprehension — flatten matrix
flat = [val for row in matrix for val in row]
print(flat)

# ------ Dict & Set Comprehensions ------
squared_dict = {x: x**2 for x in range(1, 6)}
print(squared_dict)   # {1: 1, 2: 4, 3: 9, 4: 16, 5: 25}

unique_lengths = {len(t) for t in tools}
print(unique_lengths)

# ------ Generator Expressions (memory efficient) ------
# Uses () instead of [] — lazy evaluation
gen = (x**2 for x in range(1, 1000000))  # doesn't compute all at once
print(next(gen))   # 1
print(next(gen))   # 4

# sum using generator — no intermediate list created
total = sum(x**2 for x in range(1, 101))
print(total)   # 338350

# ------ Infinite loop with break ------
import random
while True:
    n = random.randint(1, 10)
    print(f"Got: {n}")
    if n == 7:
        print("Found 7! Stopping.")
        break

# ------ Practice Exercises ------
# 1. Print the multiplication table for 7 using a for loop.
# 2. Use a while loop to find the first number > 1000 that's divisible by 13 and 7.
# 3. Write a list comprehension to get squares of odd numbers from 1 to 50.
# 4. Use zip() to create a dict from two lists: keys and values.
# 5. FizzBuzz using a list comprehension.
