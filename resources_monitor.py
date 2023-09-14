import psutil
import time
import csv
import sys
import os

# args = sys.argv[1:]
file_name = sys.argv[1]
INTERVAL = False
sleep_time = sys.argv[2]
if sleep_time != 'no_interval':
    sleep_time = float(sys.argv[2])/1000
    INTERVAL = True


if os.path.exists(file_name): os.remove(file_name)
with open(file_name, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['Time', 'CPU Usage (%)', 'RAM Usage'])

    while True:
        current_time = time.time()
        cpu_usage = psutil.cpu_percent()
        ram = psutil.virtual_memory()
        writer.writerow([current_time, cpu_usage, ram.used])
        if INTERVAL: time.sleep(sleep_time)
