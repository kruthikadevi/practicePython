# ============================================================
# 04 - Modules, Packages, and Imports
# ============================================================

# ------ Importing modules ------
import os
import sys
import math
import json
import datetime
import re

# Aliased import
import collections as col
import itertools as it

# Selective import
from pathlib import Path
from typing import List, Dict, Optional, Union, Tuple, Any
from functools import wraps, lru_cache
from collections import defaultdict, Counter, deque, OrderedDict

# ------ os module ------
print(os.getcwd())                         # current directory
print(os.path.join("/home", "kruthika", "data"))  # OS-aware path joining
print(os.path.exists("/tmp"))              # True
print(os.path.isfile("/etc/hosts"))        # True/False
print(os.path.isdir("/tmp"))               # True
print(os.path.basename("/tmp/file.csv"))   # file.csv
print(os.path.dirname("/tmp/file.csv"))    # /tmp
print(os.path.splitext("data.parquet"))    # ('data', '.parquet')
print(os.environ.get("HOME", "not set"))   # env variable
print(os.cpu_count())                      # number of CPUs

# List directory
files = os.listdir("/tmp")
print(files[:5])

# Walk a directory tree
for root, dirs, files in os.walk("/tmp"):
    for f in files:
        print(os.path.join(root, f))
    break   # just first level

# ------ pathlib (modern, preferred) ------
p = Path("/home")
data_dir = p / "kruthika" / "data"   # path joining with /
print(data_dir)
print(data_dir.parent)
print(data_dir.stem)
print(data_dir.suffix)
print(data_dir.name)

# Globbing
# for csv_file in Path(".").glob("**/*.csv"):
#     print(csv_file)

# ------ sys module ------
print(sys.version)
print(sys.platform)
print(sys.path[:3])    # module search paths
print(sys.argv)        # command-line arguments

# ------ datetime module ------
from datetime import datetime, date, timedelta, timezone

now = datetime.now()
utc_now = datetime.now(tz=timezone.utc)
today = date.today()

print(now.strftime("%Y-%m-%d %H:%M:%S"))     # format
parsed = datetime.strptime("2024-01-15", "%Y-%m-%d")  # parse

delta = timedelta(days=30)
future = now + delta
print(future.date())

# Differences
d1 = datetime(2024, 1, 1)
d2 = datetime(2024, 3, 23)
diff = d2 - d1
print(diff.days)   # 82

# ------ json module ------
data = {
    "pipeline": "ETL_Daily",
    "source": {"type": "S3", "path": "s3://bucket/data"},
    "schedule": "0 2 * * *",
    "active": True,
    "retries": 3
}

# Serialize
json_str = json.dumps(data, indent=2)
print(json_str)

# Deserialize
loaded = json.loads(json_str)
print(loaded["pipeline"])

# File I/O
# with open("config.json", "w") as f:
#     json.dump(data, f, indent=2)
# with open("config.json") as f:
#     config = json.load(f)

# ------ re (regex) module ------
text = "Email: kruthika@cognizant.com, Phone: +91-9876543210"

# Search
email_pattern = r"[\w.+-]+@[\w-]+\.[a-zA-Z]+"
match = re.search(email_pattern, text)
if match:
    print(f"Found email: {match.group()}")

# Find all
phones = re.findall(r"\+?\d[\d\-]{8,14}\d", text)
print(phones)

# Sub (replace)
cleaned = re.sub(r"\s+", " ", "too    many    spaces")
print(cleaned)

# Groups
log_line = "2024-03-23 14:32:01 ERROR Pipeline failed: connection timeout"
pattern = r"(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2}) (\w+) (.+)"
m = re.match(pattern, log_line)
if m:
    date_str, time_str, level, message = m.groups()
    print(f"Date: {date_str}, Level: {level}, Msg: {message}")

# Compiled patterns (reuse)
UUID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")

# ------ collections module ------
# Counter
words = "the quick brown fox jumps over the lazy dog the fox".split()
count = Counter(words)
print(count.most_common(3))   # [('the', 3), ('fox', 2), ...]

# defaultdict
graph = defaultdict(list)
graph["A"].append("B")
graph["A"].append("C")
graph["B"].append("D")
print(dict(graph))

# deque — double-ended queue (O(1) appendleft/popleft)
dq = deque([1, 2, 3])
dq.appendleft(0)
dq.append(4)
dq.rotate(1)         # rotate right by 1
print(dq)

# Named sliding window
window = deque(maxlen=5)
for i in range(10):
    window.append(i)
    print(list(window))

# ------ itertools module ------
from itertools import chain, islice, product, permutations, combinations, groupby, cycle, count as icount

# chain — merge iterables
merged = list(chain([1, 2], [3, 4], [5, 6]))
print(merged)

# islice — lazy slice
first5 = list(islice(range(1000000), 5))
print(first5)

# combinations / permutations
cols = ["name", "age", "city"]
print(list(combinations(cols, 2)))
print(list(permutations(cols, 2)))

# product — cartesian product
envs = ["dev", "prod"]
regions = ["us-east", "eu-west"]
print(list(product(envs, regions)))

# groupby
data = [
    {"dept": "DE", "name": "Alice"},
    {"dept": "DE", "name": "Bob"},
    {"dept": "ML", "name": "Carol"},
    {"dept": "ML", "name": "Dave"},
]
data.sort(key=lambda x: x["dept"])  # must sort first!
for dept, members in groupby(data, key=lambda x: x["dept"]):
    print(f"{dept}: {[m['name'] for m in members]}")

# ------ typing module ------
def process_records(
    records: List[Dict[str, Any]],
    schema: Optional[Dict[str, str]] = None,
    output: Union[str, Path] = "output.parquet"
) -> Tuple[int, List[str]]:
    """Returns (count, errors)."""
    errors = []
    return len(records), errors

# ------ Practice Exercises ------
# 1. Walk a directory and count files by extension using Counter.
# 2. Use re to extract all URLs from a block of text.
# 3. Use itertools.product to generate all possible Spark configs (cores × memory).
# 4. Parse a log file datetime string and calculate how many days ago it was.
# 5. Build a simple config loader that reads from a JSON file with env var overrides.
