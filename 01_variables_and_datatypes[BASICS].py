# ============================================================
# 01 - Variables and Data Types
# ============================================================
# Topics: int, float, str, bool, NoneType, type(), id()
# ============================================================

# ------ Integers ------
age = 25
year = 2024
negative = -10
big_number = 1_000_000  # underscores for readability

print(type(age))          # <class 'int'>
print(type(big_number))   # <class 'int'>

# ------ Floats ------
pi = 3.14159
temperature = -7.5
scientific = 1.5e10       # 1.5 × 10^10

print(type(pi))           # <class 'float'>

# ------ Strings ------
name = "KruthikaDevi"
city = 'Chennai'
multi_line = """This is
a multi-line
string"""

print(type(name))         # <class 'str'>
print(len(name))          # 12

# ------ Booleans ------
is_engineer = True
is_student = False

print(type(is_engineer))  # <class 'bool'>
print(int(True))          # 1  (bool is subclass of int)
print(int(False))         # 0

# ------ NoneType ------
result = None
print(type(result))       # <class 'NoneType'>
print(result is None)     # True

# ------ Dynamic Typing ------
x = 10
print(type(x))   # int
x = "hello"
print(type(x))   # str  — Python allows reassigning to a different type

# ------ Type Conversion (Casting) ------
num_str = "42"
num_int = int(num_str)        # "42" → 42
num_float = float(num_str)    # "42" → 42.0
back_to_str = str(num_int)    # 42 → "42"
bool_val = bool(0)            # 0 → False, any non-zero → True

print(num_int, num_float, back_to_str, bool_val)

# ------ Variable Naming Rules ------
# ✅ Valid
my_variable = 1
_private = 2
camelCase = 3   # works but not Pythonic
UpperCase = 4

# ❌ Invalid (would raise SyntaxError)
# 2variable = 5
# my-var = 6
# class = 7  (reserved keyword)

# ------ Multiple Assignment ------
a, b, c = 1, 2, 3
x = y = z = 0         # all point to same value
first, *rest = [1, 2, 3, 4, 5]   # unpacking

print(a, b, c)        # 1 2 3
print(x, y, z)        # 0 0 0
print(first, rest)    # 1 [2, 3, 4, 5]

# ------ Constants (by convention use UPPER_CASE) ------
MAX_RETRIES = 3
PI = 3.14159
# Python has no built-in constant enforcement, it's a naming convention

# ------ id() — memory address ------
a = 256
b = 256
print(a is b)   # True  — Python caches small integers (-5 to 256)

a = 1000
b = 1000
print(a is b)   # False — different objects in memory for large ints

# ------ Practice Exercises ------
# 1. Create variables for your name, age, height, and whether you're employed.
# 2. Print each variable's type using type().
# 3. Convert your age (int) to float and back to int.
# 4. Create a multi-line string describing yourself.
