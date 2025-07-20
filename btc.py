#! /usr/bin/python3

# by FvH, released under Apache License v2.0

# either install 'python3-paho-mqtt' or 'pip3 install paho-mqtt'

from configuration import *
from prophet import Prophet
import json
import paho.mqtt.client as mqtt
import pandas as pd
import requests
import socket
import sqlite3
import sys
import threading
import time
import traceback

con = sqlite3.connect(db_file)
cur = con.cursor()
try:
    cur.execute('CREATE TABLE price(ts datetime not null primary key, btc_price double not null)')

except sqlite3.OperationalError as oe:
    # should be "table already exists"
    pass
cur.close()

cur = con.cursor()
cur.execute('PRAGMA journal_mode=wal')
cur.close()

con.commit()

exchange_rates = None
exchange_rates_ts = 0

def announce_commands(client):
    target_topic = f'{topic_prefix}to/bot/register'

    client.publish(target_topic, 'hgrp=btc|cmd=btc|descr=Show bitcoin statistics: current timestamp, latest price (compared to previous price), lowest price (compared to > 24h back), highest price (compared to > 24h back)')
    client.publish(target_topic, 'hgrp=btc|cmd=btcprice|descr=Show the price in a currency of a certain BTC amnount. Parameters: amount and currency.')
    client.publish(target_topic, 'hgrp=btc|cmd=btcplin|descr=Linear predictions for bitcoin price')
    client.publish(target_topic, 'hgrp=btc|cmd=btcfb|descr=Predict bitcoin price using facebook-prophet')

def calc_median(rows):
    rows = sorted(rows)

    if len(rows) % 2 == 0:
        return (rows[len(rows) // 2 - 1][0] + rows[len(rows) // 2][0]) / 2.0

    return rows[len(rows) // 2][0]

def compare_prices(latest, previous, comment):
    if previous == None:
        return ''

    up   = "\003" + "3" + "\u25B2" + "\003";
    down = "\003" + "5" + "\u25BC" + "\003";

    direction = up if latest > previous else (down if latest < previous else '=')

    percentage = (latest - previous) / previous * 100.0

    if comment != '':
        comment = ' ' + comment

    return f'({direction} {percentage:.2f}%{comment})'

def predict_linear(v1, t1, v2, t2, t3):
    delta_v = v2 - v1
    delta_t = t2 - t1

    new_t = t3
    new_v = v2 + delta_v / delta_t * (t3 - t2)

    return new_t, new_v

def median(values):
    try:
        center = len(values) // 2

        if len(values) == 1:
            return (values[center][0], values[center][1])

        values.sort()

        if len(values) & 1:  # odd
            return (values[center][0], values[center][1])

        return ((values[center][0] + values[center + 1][0]) / 2, (values[center][1] + values[center + 1][1]) / 2)

    except Exception as e:
        print(f'Exception in "median()": {e}, line number: {e.__traceback__.tb_lineno}')

        return None

def sparkline(numbers):
    # bar = u'\u9601\u9602\u9603\u9604\u9605\u9606\u9607\u9608'
    bar = chr(9601) + chr(9602) + chr(9603) + chr(9604) + chr(9605) + chr(9606) + chr(9607) + chr(9608)
    barcount = len(bar)

    mn, mx = min(numbers), max(numbers)
    extent = mx - mn
    if extent != 0:
        sparkline = ''.join(bar[min([barcount - 1, int((n - mn) / extent * barcount)])] for n in numbers)
    else:
        sparkline = '- n.a. (yet) -'

    return mn, mx, sparkline

def prophet_low():
    con = sqlite3.connect(db_file)

    try:
        cur = con.cursor()
        cur.execute('SELECT strftime("%s", ts) as ts, btc_price FROM (select ts, btc_price from price order by ts desc LIMIT 20000) AS in_ ORDER BY ts')

        tsa = []
        tsm = []
        va  = []
        vm  = []

        groupby = None
        avg_tot_t = None
        avg_tot_v = None
        med_tot = None
        n_tot   = 0

        for row in cur:
            ts = int(row[0])
            v  = float(row[1])

            groupby_cur = int(ts / 300)

            if groupby_cur != groupby:
                if n_tot > 0:
                    tsa.append(avg_tot_t / n_tot)
                    va .append(avg_tot_v / n_tot)

                    med = median(med_tot)
                    if med != None:
                        tsm.append(med[0])
                        vm .append(med[1])

                groupby = groupby_cur

                n_tot     = 0
                avg_tot_v = 0
                avg_tot_t = 0

                med_tot   = []

            avg_tot_v += v
            avg_tot_t += ts
            n_tot     += 1

            med_tot.append((ts, v))

        if n_tot > 0:
            tsa.append(avg_tot_t / n_tot)
            va .append(avg_tot_v / n_tot)

            med = median(med_tot)
            if med != None:
                tsm.append(med[0])
                vm .append(med[1])

        cur.close()

        # average
        ds_a = pd.to_datetime(tsa, unit='s')
        df_a = pd.DataFrame({'ds': ds_a, 'y': va}, columns=['ds', 'y'])

        m = Prophet()
        m.fit(df_a)

        n_periods = 21

        future = m.make_future_dataframe(periods=n_periods)
        future.tail()

        forecast = m.predict(future)

        prediction_ts_a = list(forecast.tail(n_periods).head(1)['ds'])[0]
        prediction_va   = list(forecast.tail(n_periods).head(1)['yhat'])[0]

        # median
        ds_m = pd.to_datetime(tsm, unit='s')
        df_m = pd.DataFrame({'ds': ds_m, 'y': vm}, columns=['ds', 'y'])

        m = Prophet()
        m.fit(df_m)

        future = m.make_future_dataframe(periods=n_periods)
        future.tail()

        forecast = m.predict(future)

        prediction_ts_m = list(forecast.tail(n_periods).head(1)['ds'])[0]
        prediction_ma   = list(forecast.tail(n_periods).head(1)['yhat'])[0]

    except Exception as e:
        print(f'Exception while predicting BTC price (facebook prophet): {e}, line number: {e.__traceback__.tb_lineno}')
        return None, None, None, None

    con.close()

    return prediction_va, prediction_ts_a, prediction_ma, prediction_ts_m

def prophet(client, response_topic, verbose):
    try:
        client.publish(response_topic, 'Predicting takes a while, please wait.')

        prediction_va, prediction_ts_a, prediction_ma, prediction_ts_m = prophet_low()

        out = f'BTC price prediction: (probably not correct): {prediction_va:.2f} dollar (based on 5min average, {prediction_ts_a}) or {prediction_ma:.2f} dollar (based on 5min median, {prediction_ts_m})'
        client.publish(response_topic, out)

    except Exception as e:
        client.publish(response_topic, f'Exception while predicting BTC price (facebook prophet): {e}, line number: {e.__traceback__.tb_lineno}')

def btcplin():
    con = sqlite3.connect(db_file)

    try:
        cur = con.cursor()

        cur.execute('SELECT btc_price, strftime("%s", ts) FROM price ORDER BY ts DESC LIMIT 1')
        latest_btc_price, latest_epoch = cur.fetchone()
        #print(latest_btc_price, latest_epoch)

        cur.execute('SELECT btc_price, strftime("%s", ts) FROM price WHERE ts < DateTime("now", "-24 hour") ORDER BY ts DESC LIMIT 1')
        h24back_btc_price, h24back_epoch = cur.fetchone()
        #print(h24back_btc_price, h24back_epoch)

        ts, v_avg = predict_linear(float(h24back_btc_price), int(h24back_epoch), float(latest_btc_price), int(latest_epoch), int(time.time()) + 86400)

        cur.execute('SELECT btc_price, strftime("%s", ts) FROM price WHERE ts >= DateTime("now", "-24 hour") ORDER BY ts ASC')
        rows = cur.fetchall()
        median = calc_median(rows)
        ts_median = rows[0][1]

        cur.execute('SELECT btc_price, strftime("%s", ts) FROM price WHERE ts >= DateTime("now", "-48 hour") and ts < DateTime("now", "-24 hour") ORDER BY ts ASC')
        rows = cur.fetchall()
        yesterday_median = calc_median(rows)
        ts_yesterday_median = rows[0][1]

        ts, v_median = predict_linear(float(yesterday_median), int(ts_yesterday_median), float(median), int(ts_median), int(time.time()) + 86400)

        cur.close()

        con.close()

        return ts, v_avg, v_median

    except Exception as e:
        print(f'Exception while predicting BTC price: {e}, line number: {e.__traceback__.tb_lineno}')
        con.close()
        return None, None, None

def collect_predictions():
    if influx_server != None:
        while True:
            try:
                prediction_va, prediction_ts_a, prediction_ma, prediction_ts_m = prophet_low()

                ts, v_avg, v_median = btcplin()

                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect(influx_server)
                if prediction_va:
                    s.send(f'btc_usd_prophet_avg {prediction_va} {int(prediction_ts_a.to_pydatetime().timestamp())}\n'.encode('ascii'))
                if prediction_ma:
                    s.send(f'btc_usd_prophet_median {prediction_ma} {int(prediction_ts_m.to_pydatetime().timestamp())}\n'.encode('ascii'))
                if v_avg:
                    s.send(f'btc_usd_plin_avg {v_avg} {int(ts)}\n'.encode('ascii'))
                if v_median:
                    s.send(f'btc_usd_plin_median {v_median} {int(ts)}\n'.encode('ascii'))
                s.close()
            except Exception as e:
                print(f'Exception while predicting BTC price (facebook prophet): {e}, line number: {e.__traceback__.tb_lineno}')

            time.sleep(60)

def on_message_btc(client, userdata, message):
    text = message.payload.decode('utf-8')

    if message.topic == 'vanheusden/bitcoin/bitstamp_usd':
        try:
            # print(text)
            btc_price = float(text)

            cur = userdata.cursor()
            cur.execute("INSERT INTO price(ts, btc_price) VALUES(DateTime('now'), ?)", (btc_price,))
            cur.close()

            userdata.commit()

            if influx_server != None:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect(influx_server)
                s.send(f'btc_usd {btc_price} {int(time.time())}\n'.encode('ascii'))
                s.close()

        except Exception as e:
            print(f'BTC announcement failed: {e}')

def fetch_exchange_rates():
    global exchange_rates
    global exchange_rates_ts

    now = time.time()

    if now - exchange_rates_ts > 15 * 60 or exchange_rates == None:
        r = requests.get(f'https://blockchain.info/ticker')
        exchange_rates = json.loads(r.content.decode('utf8'))

        exchange_rates_ts = now

    return exchange_rates

def on_message(client, userdata, message):
    global prefix

    text = message.payload.decode('utf-8')

    topic = message.topic[len(topic_prefix):]

    if topic == 'from/bot/command' and text == 'register':
        announce_commands(client)

        return

    if topic == 'from/bot/parameter/prefix':
        prefix = text

        return

    parts   = topic.split('/')
    channel = parts[2] if len(parts) >= 3 else 'knageroe'
    nick    = parts[3] if len(parts) >= 4 else 'jemoeder'

    #print(channel)

    if channel in channels or (len(channel) >= 1 and channel[0] == '\\'):
        response_topic = f'{topic_prefix}to/irc/{channel}/notice'

        tokens  = text.split(' ')

        #print(tokens)

        command = tokens[0][1:]

        if command == 'btcprice':
            if len(tokens) != 3:
                client.publish(response_topic, f'Required parameters: btc-amount currency')
            else:
                try:
                    rates = fetch_exchange_rates()

                    if rates:
                        btc_amount = float(tokens[1])
                        currency = tokens[2].upper()

                        if currency in rates:
                            client.publish(response_topic, f'{btc_amount} BTC is ~ {rates[currency]["last"] * btc_amount:.2f} {currency}')

                        else:
                            client.publish(response_topic, f"Currency {currency} is not known, use one of: {', '.join([c for c in rates])}")

                except Exception as e:
                    print(f'Exception while !btcprice: {e}, line number: {e.__traceback__.tb_lineno}')
                    client.publish(response_topic, f'Failed updating exchange rates: {e}')

        elif command == 'btc':
            try:
                cur = con.cursor()

                cur.execute('SELECT datetime(ts, "localtime"), btc_price, strftime("%s", ts) FROM price ORDER BY ts DESC LIMIT 1')
                ts, latest_btc_price, latest_epoch = cur.fetchone()

                cur.execute('SELECT MIN(btc_price), MAX(btc_price), AVG(btc_price) FROM price WHERE ts >= DateTime("now", "-24 hour")')
                lowest_btc_price, highest_btc_price, avg_btc_price = cur.fetchone()

                cur.execute('SELECT MIN(btc_price), MAX(btc_price), AVG(btc_price) FROM price WHERE ts >= DateTime("now", "-48 hour") and ts < DateTime("now", "-24 hour")')
                yesterday_lowest_btc_price, yesterday_highest_btc_price, yesterday_avg_btc_price = cur.fetchone()

                cur.execute('SELECT btc_price FROM price WHERE ts >= DateTime("now", "-24 hour")')
                rows = cur.fetchall()
                median = calc_median(rows)

                cur.execute('SELECT btc_price FROM price WHERE ts >= DateTime("now", "-48 hour") and ts < DateTime("now", "-24 hour")')
                rows = cur.fetchall()
                yesterday_median = calc_median(rows)

                out = f'timestamp: {ts}, latest BTC price: {latest_btc_price:.2f} USD, lowest: {lowest_btc_price:.2f} {compare_prices(lowest_btc_price, yesterday_lowest_btc_price, "")} USD, highest: {highest_btc_price:.2f} USD {compare_prices(highest_btc_price, yesterday_highest_btc_price, "")}, average: {avg_btc_price:.2f} USD {compare_prices(avg_btc_price, yesterday_avg_btc_price, "")}, median: {median:.2f} USD {compare_prices(median, yesterday_median, "")}'

                if '-v' in text:
                    cur.execute('SELECT AVG(btc_price) AS btc_price FROM price WHERE ts >= DateTime("now", "-24 hour") GROUP BY ROUND(STRFTIME("%s", ts)/3600) ORDER BY ts')
                    rows = cur.fetchall()

                    values = [ row[0] for row in rows ]

                    mn, mx, sp = sparkline(values)

                    out += ' ' + sp

                cur.close()

                client.publish(response_topic, out.encode('utf-8'))

            except Exception as e:
                client.publish(response_topic, f'Problem retrieving BTC price ({e} - line number: {e.__traceback__.tb_lineno})')
                traceback.print_exc(file=sys.stderr)

        elif command == 'btcplin':
            try:
                ts, v_avg, v_median = btcplin()

                client.publish(response_topic, f'In 24 hours the bitcoin price may be around {v_avg:.2f} USD (based on average), or {v_median:.2f} USD (based on median)')

            except Exception as e:
                client.publish(response_topic, f'Exception while predicting BTC price (linear): {e}, line number: {e.__traceback__.tb_lineno}')

        elif command == 'btcfb':
            t = threading.Thread(target=prophet, args=(client, response_topic, '-v' in text), daemon=True)
            t.start()

def on_connect(client, userdata, flags, rc):
    try:
        client.subscribe(f'{topic_prefix}from/irc/#')
        client.subscribe(f'{topic_prefix}from/bot/command')
    except Exception as e:
        print(e)

def on_connect_btc(client, userdata, flags, rc):
    try:
        client.subscribe('vanheusden/bitcoin/bitstamp_usd')
    except Exception as e:
        print(e)

def announce_thread(client):
    while True:
        try:
            announce_commands(client)

            time.sleep(4.1)

        except Exception as e:
            print(f'Failed to announce: {e}')

def btc_thread():
    con = sqlite3.connect(db_file)

    btc_client = mqtt.Client(userdata=con)
    btc_client.on_message = on_message_btc
    btc_client.on_connect = on_connect_btc
    btc_client.connect(mqtt_btc_server, port=mqtt_btc_port, keepalive=4, bind_address="")

    btc_client.loop_forever()

client = mqtt.Client()
client.on_message = on_message
client.on_connect = on_connect
client.connect(mqtt_server, port=mqtt_port, keepalive=4, bind_address="")

t1 = threading.Thread(target=announce_thread, args=(client,))
t1.start()

t2 = threading.Thread(target=btc_thread)
t2.start()

t3 = threading.Thread(target=collect_predictions)
t3.start()

client.loop_forever()
