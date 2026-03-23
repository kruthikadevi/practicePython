# ============================================================
# 04 - Control Flow — if / elif / else / match
# ============================================================

# ------ Basic if / elif / else ------
score = 78

if score >= 90:
    grade = "A"
elif score >= 80:
    grade = "B"
elif score >= 70:
    grade = "C"
elif score >= 60:
    grade = "D"
else:
    grade = "F"

print(f"Score {score} → Grade {grade}")

# ------ Ternary / Inline if ------
age = 20
status = "adult" if age >= 18 else "minor"
print(status)

# Nested ternary (use sparingly — hurts readability)
x = 5
label = "positive" if x > 0 else ("zero" if x == 0 else "negative")
print(label)

# ------ Truthiness in conditions ------
name = ""
if name:
    print(f"Hello, {name}")
else:
    print("Name is empty")   # prints this

items = [1, 2, 3]
if items:
    print("List has items")

# ------ Comparison chaining ------
bmi = 22.5
if 18.5 <= bmi < 25:
    print("Normal weight")

# ------ match statement (Python 3.10+) ------
command = "quit"

match command:
    case "start":
        print("Starting...")
    case "stop" | "quit" | "exit":
        print("Shutting down.")
    case _:
        print(f"Unknown command: {command}")

# match with data types
point = (1, 0)
match point:
    case (0, 0):
        print("Origin")
    case (x, 0):
        print(f"On X-axis at x={x}")
    case (0, y):
        print(f"On Y-axis at y={y}")
    case (x, y):
        print(f"Point at ({x}, {y})")

# ------ Nested if ------
user_role = "admin"
is_active = True

if is_active:
    if user_role == "admin":
        print("Full access granted")
    elif user_role == "editor":
        print("Edit access granted")
    else:
        print("Read-only access")
else:
    print("Account is inactive")

# ------ pass statement ------
# Use pass as a placeholder when block must exist but nothing to do yet
feature_flag = False
if feature_flag:
    pass   # TODO: implement feature
else:
    print("Feature not enabled")

# ------ Real-world example ------
def classify_data_job(tool: str, size_gb: float) -> str:
    if size_gb > 1000:
        tier = "large"
    elif size_gb > 100:
        tier = "medium"
    else:
        tier = "small"

    match tool.lower():
        case "spark" | "databricks":
            engine = "distributed"
        case "pandas":
            engine = "single-node"
        case _:
            engine = "unknown"

    return f"{tier.capitalize()} job on {engine} engine ({tool})"

print(classify_data_job("Databricks", 500))
print(classify_data_job("pandas", 2))

# ------ Practice Exercises ------
# 1. Write a program that prints "Fizz" for multiples of 3,
#    "Buzz" for multiples of 5, and "FizzBuzz" for both.
# 2. Classify a BMI value into: underweight, normal, overweight, obese.
# 3. Use match to print the day type (weekday/weekend) for a given day name.
# 4. Write a login check: username must be "admin" AND password must be "1234".
