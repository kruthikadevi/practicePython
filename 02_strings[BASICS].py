# ============================================================
# 02 - Strings — Creation, Methods, Formatting, Slicing
# ============================================================

name = "KruthikaDevi Ravindran"
role = "Data Engineer"

# ------ Basic Operations ------
print(len(name))             # 22
print(name + " - " + role)  # concatenation
print(name * 2)              # repetition

# ------ Indexing & Slicing ------
s = "Python"
print(s[0])       # P   (first char)
print(s[-1])      # n   (last char)
print(s[1:4])     # yth (index 1 to 3)
print(s[:3])      # Pyt (from start to 2)
print(s[3:])      # hon (from index 3 to end)
print(s[::2])     # Pto (every 2nd char)
print(s[::-1])    # nohtyP (reversed)

# ------ Common String Methods ------
text = "  Hello, World!  "

print(text.strip())          # "Hello, World!"   — remove whitespace
print(text.lstrip())         # "Hello, World!  " — left strip
print(text.rstrip())         # "  Hello, World!" — right strip
print(text.lower())          # "  hello, world!  "
print(text.upper())          # "  HELLO, WORLD!  "
print(text.title())          # "  Hello, World!  "
print(text.strip().replace("World", "Python"))  # "Hello, Python!"

sentence = "the quick brown fox"
print(sentence.split())                     # ['the', 'quick', 'brown', 'fox']
print(sentence.split(" ", 2))              # ['the', 'quick', 'brown fox']
print("-".join(["a", "b", "c"]))           # "a-b-c"

print("hello world".find("world"))         # 6  (index), -1 if not found
print("hello world".index("world"))        # 6  (raises ValueError if not found)
print("hello".startswith("he"))            # True
print("hello".endswith("lo"))              # True
print("hello world".count("l"))            # 3
print("  ".isspace())                      # True
print("hello123".isalnum())               # True
print("hello".isalpha())                  # True
print("123".isdigit())                    # True

# ------ String Formatting ------

# 1. % formatting (old style)
msg = "Hello, %s! You are %d years old." % ("Kruthika", 28)
print(msg)

# 2. .format() method
msg = "Hello, {}! You are {} years old.".format("Kruthika", 28)
msg2 = "Hello, {name}! You are {age} years old.".format(name="Kruthika", age=28)
print(msg)
print(msg2)

# 3. f-strings (Python 3.6+) — RECOMMENDED
name = "Kruthika"
age = 28
experience = 7
msg = f"Hello, {name}! You have {experience} years of experience."
print(msg)

# f-string expressions
print(f"2 + 2 = {2 + 2}")
print(f"Name upper: {name.upper()}")
print(f"Pi to 2 decimals: {3.14159:.2f}")
print(f"Large number: {1000000:,}")     # 1,000,000
print(f"Padded: {'hello':>10}")         # right-align in 10 chars
print(f"Padded: {'hello':<10}|")        # left-align

# ------ Raw Strings ------
path = r"C:\Users\kruthika\Documents"   # r prefix ignores escape sequences
print(path)

# ------ Escape Characters ------
print("Line1\nLine2")     # newline
print("Tab\there")        # tab
print("Quote: \"hello\"") # escaped quote
print("Backslash: \\")    # backslash

# ------ String Immutability ------
s = "hello"
# s[0] = "H"  # ❌ TypeError: 'str' object does not support item assignment
s = "H" + s[1:]  # ✅ Create a new string instead
print(s)  # Hello

# ------ Multiline and Triple Quotes ------
sql = """
    SELECT *
    FROM engineers
    WHERE experience > 5
"""
print(sql)

# ------ Practice Exercises ------
# 1. Reverse the string "DataEngineer" using slicing.
# 2. Count how many times 'a' appears in "KruthikaDevi Ravindran".
# 3. Format your name, role, and years of experience using an f-string.
# 4. Check if "pyspark" is in "I work with PySpark daily".lower().
# 5. Split "AWS,GCP,Azure,Databricks" by comma and print each tool on a new line.
