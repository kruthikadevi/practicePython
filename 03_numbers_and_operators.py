# ============================================================
# 03 - Numbers & Operators
# ============================================================
# Topics: arithmetic, comparison, logical, bitwise, assignment,
#         operator precedence, math module
# ============================================================

import math

# ------ Arithmetic Operators ------
a, b = 17, 5

print(a + b)    # 22  — addition
print(a - b)    # 12  — subtraction
print(a * b)    # 85  — multiplication
print(a / b)    # 3.4 — true division (always float)
print(a // b)   # 3   — floor division (integer result)
print(a % b)    # 2   — modulo (remainder)
print(a ** b)   # 1419857 — exponentiation (17^5)

# ------ Comparison Operators (return bool) ------
print(5 == 5)   # True
print(5 != 4)   # True
print(5 > 3)    # True
print(5 < 3)    # False
print(5 >= 5)   # True
print(5 <= 4)   # False

# Chaining comparisons (Pythonic)
x = 7
print(1 < x < 10)    # True
print(0 < x < 5)     # False

# ------ Logical Operators ------
print(True and False)   # False
print(True or False)    # True
print(not True)         # False

# Short-circuit evaluation
# 'and' stops at first False
# 'or'  stops at first True
print(0 and 1/0)   # 0  — does NOT raise ZeroDivisionError
print(1 or 1/0)    # 1  — does NOT raise ZeroDivisionError

# Truthy / Falsy values
falsy = [0, 0.0, "", [], {}, set(), None, False]
for val in falsy:
    print(f"{repr(val):10} → {bool(val)}")

# ------ Assignment Operators ------
n = 10
n += 3   # n = n + 3  → 13
n -= 2   # n = n - 2  → 11
n *= 4   # n = n * 4  → 44
n //= 3  # n = n // 3 → 14
n **= 2  # n = n ** 2 → 196
n %= 50  # n = n % 50 → 46
print(n) # 46

# Walrus operator := (Python 3.8+) — assign and return in expression
import random
numbers = [random.randint(1, 100) for _ in range(10)]
if (n := len(numbers)) > 5:
    print(f"List has {n} elements, which is more than 5.")

# ------ Bitwise Operators ------
x, y = 0b1010, 0b1100   # 10, 12 in binary
print(bin(x & y))   # 0b1000  — AND
print(bin(x | y))   # 0b1110  — OR
print(bin(x ^ y))   # 0b0110  — XOR
print(bin(~x))       # -0b1011 — NOT (bitwise complement)
print(bin(x << 1))  # 0b10100 — left shift (multiply by 2)
print(bin(x >> 1))  # 0b101   — right shift (divide by 2)

# ------ Identity and Membership Operators ------
a = [1, 2, 3]
b = [1, 2, 3]
c = a

print(a == b)    # True  — same value
print(a is b)    # False — different objects in memory
print(a is c)    # True  — same object

print(2 in a)    # True
print(5 not in a) # True

# ------ Operator Precedence (PEMDAS/BODMAS) ------
# ** > unary - > * / // % > + - > << >> > & > ^ > | > comparisons > not > and > or
print(2 + 3 * 4)      # 14  (not 20)
print((2 + 3) * 4)    # 20
print(2 ** 3 ** 2)    # 512 (right-associative: 2 ** (3**2) = 2**9)
print(-2 ** 2)        # -4  (-(2**2), not (-2)**2)

# ------ Math Module ------
print(math.sqrt(144))       # 12.0
print(math.ceil(3.2))       # 4
print(math.floor(3.8))      # 3
print(math.fabs(-7.5))      # 7.5 (float absolute value)
print(abs(-7))              # 7   (built-in, works on int too)
print(math.pow(2, 10))      # 1024.0 (float)
print(math.log(math.e))     # 1.0
print(math.log10(1000))     # 3.0
print(math.log2(8))         # 3.0
print(math.pi)              # 3.141592...
print(math.e)               # 2.718281...
print(math.factorial(5))    # 120
print(math.gcd(12, 18))     # 6
print(math.isfinite(1.0))   # True
print(math.isinf(float('inf')))  # True
print(math.isnan(float('nan')))  # True

# round() built-in
print(round(3.14159, 2))   # 3.14
print(round(2.5))           # 2  (banker's rounding)
print(round(3.5))           # 4

# divmod() — returns (quotient, remainder) in one call
print(divmod(17, 5))    # (3, 2)

# ------ Practice Exercises ------
# 1. Without a calculator: what is 2 ** 10? Verify with Python.
# 2. Write a one-liner that checks if a number is even using %.
# 3. Use the walrus operator to filter numbers > 50 from a list comprehension.
# 4. What is math.log(math.e ** 5)?  Predict before running.
# 5. Find the GCD and LCM of 24 and 36 using math module.
