#! /usr/bin/python3

# by FvH, released under MIT License

import datetime
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import math
import numpy as np
import scipy.stats
import sqlite3


db_file = 'btc.db'
steps = 30  # group by this number of seconds

# load data
con = sqlite3.connect(db_file)
cur = con.cursor()
cur.execute('SELECT ts, btc_price FROM price ORDER BY ts')
data = dict()
date = None
day = []
for row in cur:
    parts = row[0].split()
    now_date = parts[0]
    if now_date != date:
        if date != None:
            data[date] = day
        day = dict()
        date = now_date

    parts = parts[1].split(':')
    time = int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
    rounded_time = int(time / 30) * 30
    price = float(row[1])
    if price > 0:
        day[rounded_time] = price
cur.close()
con.close()

def find_best_day(day1):
    best_value = 0
    best_day = None
    for day2 in data:
        if day2 == day1:
            continue

        # match timestamps
        x = []
        y = []
        for ts in data[day1]:
            if ts in data[day2]:
                x.append(data[day1][ts])
                y.append(data[day2][ts])

        assert len(x) == len(y)

        p = scipy.stats.pearsonr(x, y).statistic
        p_abs = math.fabs(p)
        if p_abs > best_value:
            best_value = p_abs
            best_day = day2

    return (best_day, best_value, x, y)

# day1_data: yesterday
# day2_data: best matching with yesterday
# next_day: day after day2_data, must be adjusted
def massage_data(day1_data, day2_data, next_day):
    min1 = min(day1_data)
    max1 = max(day1_data)
    min2 = min(day2_data)
    max2 = max(day2_data)

    maxn = -10
    minn = 100000000000000000000
    for ts in next_day:
        minn = min(minn, next_day[ts])
        maxn = max(maxn, next_day[ts])

    # divide yesterday by best matcher
    mul = (max1 - min1) / (max2 - min2)

    today = datetime.date.today()
    now_offset = datetime.datetime(today.year, today.month, today.day)
    out = dict()
    for ts in next_day:
        new_ts = now_offset + datetime.timedelta(seconds=ts)
        out[new_ts] = (next_day[ts] - minn) / mul + min2

    return out

def get_next_day_data(after_day, data):
    parts = after_day.split('-')
    python_datetime = datetime.datetime(int(parts[0]), int(parts[1]), int(parts[2])) + datetime.timedelta(days=1)
    next_day_date = f'{python_datetime.year:04d}-{python_datetime.month:02d}-{python_datetime.day:02d}'
    return data[next_day_date]

yesterday_date = datetime.date.today() - datetime.timedelta(days = 1)
best = find_best_day(str(yesterday_date))
next_day = get_next_day_data(best[0], data)
result = massage_data(best[2], best[3], next_day)

x = np.array([ts for ts in result])
y = np.array([result[ts] for ts in result])
plt.figure(figsize=[24, 16])
fmt = mdates.DateFormatter('%Y-%m-%d %H:%M')        # format the datetime with '%Y-%m-%d
plt.gca().xaxis.set(major_formatter=fmt)
plt.grid()
plt.plot(x,y)
plt.savefig('btc-prediction.svg')
