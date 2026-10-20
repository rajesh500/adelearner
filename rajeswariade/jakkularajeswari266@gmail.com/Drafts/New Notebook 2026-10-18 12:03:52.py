# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
summer_reading = [
    {"student": "Ravi", "book": "Harry Potter"},
    {"student": "Sita", "book": "Charlotte's Web"},
    {"student": "Ravi", "book": "The Hobbit"},
    {"student": "John", "book": "Wonder"},
    {"student": "Sita", "book": "Matilda"},
    {"student": "Ravi", "book": "The Jungle Book"}
]

# COMMAND ----------

d = {}
for i in summer_reading:
    stu = i['student']
    book = i['book']
    if stu not in d:
        d[stu] = 1
    else:
        d[stu] = d.get(stu, 0)+1
print(d)


# COMMAND ----------

summer_reading = [
    {"student": "Ravi", "book": "Harry Potter", "pages": 300},
    {"student": "Sita", "book": "Charlotte's Web", "pages": 200},
    {"student": "Ravi", "book": "The Hobbit", "pages": 250},
    {"student": "John", "book": "Wonder", "pages": 150},
    {"student": "Sita", "book": "Matilda", "pages": 180},
    {"student": "Ravi", "book": "The Jungle Book", "pages": 220}
]

# COMMAND ----------

total = 0
d = {}
for i in summer_reading:
    stu = i['student']
    if stu not in d:
        d[stu] = int(i['pages'])
    else:
        d[stu] = int(d[stu]) + int(i['pages'])
    # total = total + i['pages'] 
print(d)


# COMMAND ----------

import requests
url = input("Enter your url").strip()
n = int(input("Enter the number of url's you want to display"))
response = requests.get(url, timeout = 60)

# with open("/Volumes/rajeswariade/bronze/my_managed_volume/sampleurl.txt", 'wb') as file:
#     file.write(response.content)
d = {}
data= []
with open('/Volumes/rajeswariade/bronze/my_managed_volume/sampleurl.txt', 'r', newline= '') as file:
    for i in file:
        parts = i.split("//")
        if len(parts) < 2:
            continue
        ss = parts[1]
        s = ss.split('/')[0]
        if s not in d:
            d[s] = int(1)
        else:
            d[s] = int(d[s])+1
d1 = dict(sorted(d.items(), key = lambda x:x[1], reverse =True))

count = 1
for k, v in d1.items():
    if count <= n:
        print(k, v)
    count = count + 1


# COMMAND ----------

with open("/Volumes/rajeswariade/bronze/my_managed_volume/block.txt", "r") as file:
    bcount = 0
    rcount = 0
    for i in file:
        rowList = i.split()
        if 'block' in rowList:
            bcount +=1
        else:
            rcount +=1

print("block count", bcount, "release count", rcount)

# COMMAND ----------

from datetime import datetime
rowList = []
cutoff = datetime(2026, 7, 1, 2, 0, 0)
print(cutoff)
with open("/Volumes/rajeswariade/bronze/my_managed_volume/block.txt", "r") as file:
    for i in file:
        datetext, timestamptext, status, ip = i.split()
        #print(datetext, timestamptext, status, ip)
        ts = datetime.strptime(f"{datetext} {timestamptext}", "%Y-%m-%d %H:%M:%S")
        #print(ts)
        rowList.append((ts, status, ip))
        #print(rowList)

#print(rowList)
d = {}

for j in rowList:
    if j[0] < cutoff:
        print(j[0], j[1], j[2])
        ipd = j[2]
        statusd = j[1]
        # count events per status for each IP
        if ipd not in d:
            d[ipd] = {}
        d[ipd][statusd] = d[ipd].get(statusd, 0) + 1

print(d)
# for ip, status_counts in d.items():
#     print(ip, status_counts)

# COMMAND ----------

from datetime import datetime

# Find the status at 2 AM on this date, using the log's timezone.
cutoff = datetime(2026, 7, 1, 2, 0, 0)

events = []

# Read and parse the log file.
with open("/Volumes/rajeswariade/bronze/my_managed_volume/block.txt", "r", encoding="utf-8") as file:
    for line in file:
        if not line.strip():
            continue

        date_text, time_text, action, ip = line.split()
        # print(date_text, time_text, action, ip, '1')

        timestamp = datetime.strptime(
            f"{date_text} {time_text}",
            "%Y-%m-%d %H:%M:%S"
        )
        # print(timestamp, '2')
        events.append((timestamp, action, ip))
        # print(events, '3')

# Process events in chronological order.
events.sort(key=lambda event: event[0])
# print(events, '4')

blocked_ips = set()
counts = {}

for timestamp, action, ip in events:
    if timestamp > cutoff:
        break

    if ip not in counts:
        counts[ip] = {"block": 0, "release": 0}

    counts[ip][action] += 1

    if action == "block":
        blocked_ips.add(ip)
    elif action == "release":
        blocked_ips.discard(ip)

# Display event counts and status for each IP.
print(f"{'IP address':<18} {'Blocks':<8} {'Releases':<10} Status")

for ip in sorted(counts):
    status = "Blocked" if ip in blocked_ips else "Released"

    print(
        f"{ip:<18} "
        f"{counts[ip]['block']:<8} "
        f"{counts[ip]['release']:<10} "
        f"{status}"
    )

print("\nTotal IPs still blocked:", len(blocked_ips))
print("Total block events:", sum(c["block"] for c in counts.values()))
print("Total release events:", sum(c["release"] for c in counts.values()))

# COMMAND ----------

summer_reading = [
    {"reader_id": 101, "pages": 300},
    {"reader_id": 102, "pages": 200},
    {"reader_id": 101, "pages": 250},
    {"reader_id": 103, "pages": 150},
    {"reader_id": 102, "pages": 180},
    {"reader_id": 101, "pages": 220},
    {"reader_id": 104, "pages": 400},
    {"reader_id": 105, "pages": 320},
    {"reader_id": 106, "pages": 100}
]

# COMMAND ----------

d = {}
for i in summer_reading:
    readid = i['reader_id']
    pageno = i['pages']
    if readid not in d:
        if pageno > 25:
            d[readid] = pageno + 50
    else:
        d[readid] = d.get(readid, 0) + pageno + 50

d1 = dict(sorted(d.items(), key=lambda x:x[1], reverse = True))
count = 1
for k, v in d1.items():
    if count <= 5:
        print(k, v)
    count = count + 1