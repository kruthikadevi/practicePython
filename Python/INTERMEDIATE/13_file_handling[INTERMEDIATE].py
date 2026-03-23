# ============================================================
# 05 - File Handling — Read, Write, CSV, JSON, Pathlib
# ============================================================

import os
import csv
import json
from pathlib import Path

WORK_DIR = Path("/tmp/python_files_demo")
WORK_DIR.mkdir(parents=True, exist_ok=True)

# ------ Writing Text Files ------
txt_path = WORK_DIR / "sample.txt"

# Basic write (overwrites)
with open(txt_path, "w") as f:
    f.write("Line 1\n")
    f.write("Line 2\n")
    f.writelines(["Line 3\n", "Line 4\n", "Line 5\n"])

# Append mode
with open(txt_path, "a") as f:
    f.write("Line 6 (appended)\n")

# ------ Reading Text Files ------
# Read all at once
with open(txt_path, "r") as f:
    content = f.read()
    print(content)

# Read line by line (memory efficient for large files)
with open(txt_path, "r") as f:
    for line in f:
        print(line.rstrip())   # strip trailing newline

# Read all lines into a list
with open(txt_path, "r") as f:
    lines = f.readlines()     # includes \n
    lines = [l.rstrip() for l in lines]

# readline() — one line at a time
with open(txt_path, "r") as f:
    first = f.readline()
    second = f.readline()
    print(first, second)

# ------ File Modes ------
# "r"   — read (default)
# "w"   — write (creates/overwrites)
# "a"   — append
# "x"   — exclusive create (fails if exists)
# "b"   — binary mode (combine: "rb", "wb")
# "+"   — read+write: "r+" (don't truncate), "w+" (truncate)

# Binary files
bin_path = WORK_DIR / "data.bin"
with open(bin_path, "wb") as f:
    f.write(b"\x00\x01\x02\x03\xFF")

with open(bin_path, "rb") as f:
    data = f.read()
    print(list(data))   # [0, 1, 2, 3, 255]

# ------ Encoding ------
utf_path = WORK_DIR / "unicode.txt"
with open(utf_path, "w", encoding="utf-8") as f:
    f.write("Hello 🌍 Tamil: தமிழ்\n")

with open(utf_path, "r", encoding="utf-8") as f:
    print(f.read())

# ------ CSV Files ------
csv_path = WORK_DIR / "engineers.csv"

# Write CSV
fieldnames = ["name", "role", "experience", "team"]
rows = [
    {"name": "KruthikaDevi", "role": "Data Engineer", "experience": 7, "team": "DE"},
    {"name": "Alice",        "role": "ML Engineer",   "experience": 4, "team": "ML"},
    {"name": "Bob",          "role": "DevOps",        "experience": 6, "team": "Infra"},
]

with open(csv_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)

# Read CSV as dicts
with open(csv_path, "r", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        print(dict(row))

# CSV with custom delimiter
tsv_path = WORK_DIR / "data.tsv"
with open(tsv_path, "w", newline="") as f:
    writer = csv.writer(f, delimiter="\t")
    writer.writerow(["id", "value", "label"])
    writer.writerows([[1, 10.5, "A"], [2, 20.1, "B"]])

# ------ JSON Files ------
json_path = WORK_DIR / "pipeline_config.json"

pipeline_config = {
    "name": "ETL_Daily",
    "version": "2.1.0",
    "schedule": {"cron": "0 2 * * *", "timezone": "UTC"},
    "source": {"type": "S3", "bucket": "raw-data", "prefix": "orders/"},
    "sink":   {"type": "Databricks", "catalog": "silver", "table": "orders"},
    "dq_checks": ["not_null", "unique_id", "row_count > 0"],
    "active": True,
    "max_retries": 3
}

# Write JSON
with open(json_path, "w") as f:
    json.dump(pipeline_config, f, indent=2)

# Read JSON
with open(json_path, "r") as f:
    loaded = json.load(f)

print(loaded["name"])
print(loaded["source"]["bucket"])

# Pretty-print from file
print(json.dumps(loaded, indent=2)[:200])

# ------ Pathlib Advanced ------
# Create dirs
logs_dir = WORK_DIR / "logs" / "2024" / "03"
logs_dir.mkdir(parents=True, exist_ok=True)

# Write & read with Path
log_file = logs_dir / "pipeline.log"
log_file.write_text("Pipeline started\nPipeline finished\n")
print(log_file.read_text())

# Glob patterns
for f in WORK_DIR.glob("*.csv"):
    print(f"CSV: {f.name}, size: {f.stat().st_size} bytes")

for f in WORK_DIR.rglob("*"):   # recursive
    if f.is_file():
        print(f.relative_to(WORK_DIR))

# Path properties
p = Path("/home/kruthika/data/orders.parquet")
print(p.name)       # orders.parquet
print(p.stem)       # orders
print(p.suffix)     # .parquet
print(p.parent)     # /home/kruthika/data
print(p.parts)      # ('/', 'home', 'kruthika', 'data', 'orders.parquet')
print(p.is_absolute())  # True

# Rename / move
# p.rename(p.with_suffix(".csv"))
# p.replace(Path("/new/path/orders.parquet"))

# ------ Large File Processing (line by line) ------
def process_large_csv(path: Path, chunk_size: int = 1000):
    """Process a large CSV without loading it all into memory."""
    processed = 0
    with open(path, "r") as f:
        reader = csv.DictReader(f)
        batch = []
        for row in reader:
            batch.append(row)
            if len(batch) >= chunk_size:
                # process batch here
                processed += len(batch)
                batch = []
        if batch:
            processed += len(batch)
    return processed

count = process_large_csv(csv_path)
print(f"Processed {count} rows")

# ------ tempfile ------
import tempfile

with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
    json.dump({"temp": True}, tmp)
    tmp_path = tmp.name

print(f"Temp file: {tmp_path}")
os.unlink(tmp_path)  # clean up

# ------ Practice Exercises ------
# 1. Read a CSV and group rows by a column, writing each group to a separate file.
# 2. Write a function that merges multiple JSON config files with precedence rules.
# 3. Build a simple file-based logger that rotates daily.
# 4. Count word frequency in a large text file without reading it all at once.
# 5. Convert a CSV file to JSON format preserving types (int, float, bool).
