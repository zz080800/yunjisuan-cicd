import threading
import requests
import time

url = "http://139.9.117.181/api/ping"
total_requests = 10000
concurrent_threads = 200
completed = 0

def send_request():
    global completed
    try:
        requests.get(url, timeout=5)
    except:
        pass
    completed += 1

print(f"Starting load test to {url}")
print(f"Total: {total_requests}, Concurrency: {concurrent_threads}")

start_time = time.time()
threads = []
for i in range(total_requests):
    t = threading.Thread(target=send_request)
    t.start()
    threads.append(t)
    if i % 100 == 0:
        print(f"Started {i} requests")

for t in threads:
    t.join()

elapsed = time.time() - start_time
print(f"Completed {completed} requests in {elapsed:.2f} seconds")