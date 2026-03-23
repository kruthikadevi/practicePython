# ============================================================
# 03 - Concurrency — Threading, Multiprocessing, Asyncio
# ============================================================

import time
import threading
import multiprocessing
import asyncio
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed

# ============================================================
# THREADING — I/O-bound tasks (network, file, DB)
# GIL prevents true parallelism for CPU, but fine for I/O
# ============================================================

def download_file(url: str, delay: float = 1.0) -> str:
    """Simulate downloading a file."""
    print(f"[Thread-{threading.current_thread().name}] Downloading {url}")
    time.sleep(delay)
    return f"data_from_{url.split('/')[-1]}"

# Sequential
urls = [f"https://data.example.com/file_{i}.csv" for i in range(5)]

start = time.time()
results = [download_file(url, 0.3) for url in urls]
print(f"Sequential: {time.time() - start:.2f}s → {len(results)} files")

# With threads
start = time.time()
threads = []
results = {}

for url in urls:
    t = threading.Thread(target=lambda u=url: results.update({u: download_file(u, 0.3)}))
    threads.append(t)
    t.start()

for t in threads:
    t.join()

print(f"Threaded:   {time.time() - start:.2f}s → {len(results)} files")

# ------ ThreadPoolExecutor (preferred) ------
with ThreadPoolExecutor(max_workers=5) as executor:
    futures = {executor.submit(download_file, url, 0.3): url for url in urls}
    for future in as_completed(futures):
        url = futures[future]
        try:
            result = future.result()
            print(f"Got: {result}")
        except Exception as e:
            print(f"Failed {url}: {e}")

# ------ Thread Safety ------
counter = 0
lock = threading.Lock()

def unsafe_increment():
    global counter
    for _ in range(10_000):
        counter += 1   # ← NOT thread-safe (race condition)

def safe_increment():
    global counter
    for _ in range(10_000):
        with lock:
            counter += 1   # ← thread-safe

# Thread-safe Queue
from queue import Queue

task_queue = Queue()
results_queue = Queue()

def worker():
    while True:
        item = task_queue.get()
        if item is None:
            break
        result = item ** 2
        results_queue.put(result)
        task_queue.task_done()

num_workers = 4
workers = [threading.Thread(target=worker, daemon=True) for _ in range(num_workers)]
for w in workers:
    w.start()

for i in range(20):
    task_queue.put(i)

task_queue.join()  # wait until all tasks done

for _ in range(num_workers):
    task_queue.put(None)   # signal workers to stop

squares = []
while not results_queue.empty():
    squares.append(results_queue.get())
print(sorted(squares))

# ============================================================
# MULTIPROCESSING — CPU-bound tasks (data transformation, ML)
# True parallelism — each process has its own GIL
# ============================================================

def cpu_heavy(n: int) -> int:
    """Simulate CPU-intensive work."""
    return sum(i**2 for i in range(n))

numbers = [1_000_000] * 8

# Sequential
start = time.time()
results = [cpu_heavy(n) for n in numbers]
print(f"Sequential CPU: {time.time() - start:.2f}s")

# Multiprocessing Pool
start = time.time()
with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
    results = pool.map(cpu_heavy, numbers)
print(f"Multiprocess:   {time.time() - start:.2f}s")

# ProcessPoolExecutor
def process_partition(partition_id: int, records: list) -> dict:
    return {
        "partition": partition_id,
        "count": len(records),
        "sum": sum(r["value"] for r in records)
    }

partitions = [
    (i, [{"value": j} for j in range(1000)])
    for i in range(4)
]

with ProcessPoolExecutor(max_workers=4) as executor:
    futures = [executor.submit(process_partition, pid, recs)
               for pid, recs in partitions]
    for f in as_completed(futures):
        print(f.result())

# Shared memory
from multiprocessing import Value, Array

shared_counter = Value("i", 0)   # 'i' = signed integer
shared_array = Array("d", [0.0, 0.0, 0.0])   # 'd' = double

# ============================================================
# ASYNCIO — Async/Await (I/O concurrency in a single thread)
# ============================================================

async def async_fetch(url: str, delay: float) -> str:
    """Simulate async HTTP request."""
    print(f"Start fetching {url}")
    await asyncio.sleep(delay)   # non-blocking wait
    print(f"Done fetching {url}")
    return f"response_from_{url}"

async def fetch_all(urls: list) -> list:
    tasks = [async_fetch(url, 0.3) for url in urls]
    results = await asyncio.gather(*tasks)   # run concurrently
    return results

# Run coroutines
urls = [f"https://api.example.com/data/{i}" for i in range(5)]
results = asyncio.run(fetch_all(urls))
print(results)

# ------ Async patterns ------
async def producer(queue: asyncio.Queue, items: list):
    for item in items:
        await queue.put(item)
        await asyncio.sleep(0.01)
    await queue.put(None)   # sentinel

async def consumer(queue: asyncio.Queue, worker_id: int):
    results = []
    while True:
        item = await queue.get()
        if item is None:
            await queue.put(None)   # re-queue for other consumers
            break
        results.append(item * 2)
        await asyncio.sleep(0.005)
    print(f"Worker {worker_id} processed {len(results)} items")
    return results

async def async_pipeline():
    queue = asyncio.Queue(maxsize=10)
    items = list(range(50))

    prod = asyncio.create_task(producer(queue, items))
    consumers = [asyncio.create_task(consumer(queue, i)) for i in range(3)]

    await prod
    results = await asyncio.gather(*consumers)
    return [item for batch in results for item in batch]

all_results = asyncio.run(async_pipeline())
print(f"Processed {len(all_results)} items total")

# ------ Async context manager ------
class AsyncDatabasePool:
    async def __aenter__(self):
        print("Opening DB pool")
        await asyncio.sleep(0.01)   # simulate connection time
        return self

    async def __aexit__(self, *args):
        print("Closing DB pool")

    async def query(self, sql):
        await asyncio.sleep(0.01)
        return [{"id": 1, "result": sql}]

async def run_queries():
    async with AsyncDatabasePool() as pool:
        result = await pool.query("SELECT * FROM orders")
        print(result)

asyncio.run(run_queries())

# ------ Async generator ------
async def async_data_stream(n: int):
    for i in range(n):
        await asyncio.sleep(0.01)
        yield {"record_id": i, "value": i * 10}

async def consume_stream():
    async for record in async_data_stream(5):
        print(record)

asyncio.run(consume_stream())

# ------ Choosing the right tool ------
print("""
Choose concurrency model:
  Threading       → I/O-bound (HTTP calls, DB queries, file ops), shared memory
  Multiprocessing → CPU-bound (data transforms, ML inference, encryption)
  Asyncio         → I/O-bound with many concurrent connections, event-driven
  
Data Engineering context:
  • PySpark handles its own parallelism internally
  • Use asyncio for async API calls / webhooks
  • Use ThreadPoolExecutor for parallel file uploads/downloads
  • Use ProcessPoolExecutor for CPU-heavy pre-processing before Spark
""")

# ------ Practice Exercises ------
# 1. Download 10 URLs concurrently using ThreadPoolExecutor and time it.
# 2. Use multiprocessing to process a large list in parallel chunks.
# 3. Write an async function that calls 3 "APIs" concurrently with asyncio.gather.
# 4. Build a producer-consumer pipeline using asyncio.Queue.
# 5. Write a thread-safe counter using threading.Lock.
