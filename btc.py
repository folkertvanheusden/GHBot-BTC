#! /usr/bin/python3

# by FvH, released under Apache License v2.0

# either install 'python3-paho-mqtt' or 'pip3 install paho-mqtt'

import paho.mqtt.client as mqtt
import pandas as pd
from prophet import Prophet
import sqlite3
import threading
import time

mqtt_server    = 'mqtt.vm.nurd.space'
topic_prefix   = 'GHBot/'
channels       = ['nurdbottest', 'nurds', 'nurdsbofh']
db_file        = 'btc.db'
prefix         = '!'

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

def announce_commands(client):
    target_topic = f'{topic_prefix}to/bot/register'

    client.publish(target_topic, 'cmd=btc|descr=Show bitcoin statistics: current timestamp, latest price (compared to previous price), lowest price (compared to > 24h back), highest price (compared to > 24h back)')
    client.publish(target_topic, 'cmd=btcplin|descr=Linear predictions for bitcoin price')
    client.publish(target_topic, 'cmd=btcfb|descr=Predict bitcoin price using facebook-prophet')

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
    if len(values) == 1:
        return values[0]

    values.sort()

    center = len(values) // 2

    if len(values) & 1:  # odd
        return values[center]

    return (values[center] + values[center + 1]) / 2

def prophet(client, response_topic):
    con = sqlite3.connect(db_file)

    try:
        client.publish(response_topic, 'Predicting takes a while, please wait.')

        cur = con.cursor()
        # cur.execute('SELECT strftime("%s", ts) as ts, btc_price FROM (select ts, avg(btc_price) as btc_price from price group by round(strftime("%s", ts) / 300) order by ts desc LIMIT 1000) AS in_ ORDER BY ts')
        cur.execute('SELECT strftime("%s", ts) as ts, btc_price FROM (select ts, btc_price from price order by ts desc LIMIT 20000) AS in_ ORDER BY ts')
        rows = cur.fetchall()
        cur.close()

        ts = []
        va = []
        vm = []

        groupby = None
        avg_tot = None
        med_tot = None
        n_tot   = 0

        for row in rows:
            groupby_cur = int(int(row[0]) / 300)

            if groupby_cur != groupby:
                if n_tot > 0:
                    ts.append(groupby * 300)  # first ts
                    va.append(avg_tot / n_tot)
                    vm.append(median(med_tot))

                groupby = groupby_cur

                n_tot   = avg_tot = 0
                med_tot = []

            v = float(row[1])

            avg_tot += v
            med_tot.append(v)

            n_tot += 1

        if n_tot > 0:
            ts.append(groupby * 300)  # first ts
            va.append(avg_tot / n_tot)
            vm.append(median(med_tot))

        ds = pd.to_datetime(ts, unit='s')

        # average
        df_a = pd.DataFrame({'ds': ds, 'y': va}, columns=['ds', 'y'])

        m = Prophet()
        m.fit(df_a)

        future = m.make_future_dataframe(periods=1)
        future.tail()

        forecast = m.predict(future)

        prediction_ts = list(forecast.tail(1)['ds'])[0]
        prediction_va = list(forecast.tail(1)['trend'])[0]

        # median
        df_m = pd.DataFrame({'ds': ds, 'y': vm}, columns=['ds', 'y'])

        m = Prophet()
        m.fit(df_m)

        future = m.make_future_dataframe(periods=1)
        future.tail()

        forecast = m.predict(future)

        prediction_ma = list(forecast.tail(1)['trend'])[0]

        client.publish(response_topic, f'BTC price prediction for {prediction_ts}: (probably not correct): {prediction_va:.2f} dollar (based on 5min average) or {prediction_ma:.2f} dollar (based on 5min median)')

    except Exception as e:
        client.publish(response_topic, f'Exception while predicting BTC price (facebook prophet): {e}, line number: {e.__traceback__.tb_lineno}')

    con.close()

def on_message(client, userdata, message):
    global prefix

    text = message.payload.decode('utf-8')

    topic = message.topic[len(topic_prefix):]

    if message.topic == 'vanheusden/bitcoin/bitstamp_usd':
        try:
            btc_price = float(text)

            cur = con.cursor()
            cur.execute("INSERT INTO price(ts, btc_price) VALUES(DateTime('now'), ?)", (btc_price,))
            cur.close()

            con.commit()

        except Exception as e:
            print(f'BTC announcement failed: {e}')

        return

    if topic == 'from/bot/command' and text == 'register':
        announce_commands(client)

        return

    if topic == 'from/bot/parameter/prefix':
        prefix = text

        return

    parts   = topic.split('/')
    channel = parts[2] if len(parts) >= 3 else 'nurds'
    nick    = parts[3] if len(parts) >= 4 else 'jemoeder'

    #print(channel)

    if channel in channels:
        response_topic = f'{topic_prefix}to/irc/{channel}/notice'

        tokens  = text.split(' ')

        #print(tokens)

        command = tokens[0][1:]

        if command == 'btc':
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

                cur.close()

                client.publish(response_topic, f'timestamp: {ts}, latest BTC price: {latest_btc_price:.2f} USD, lowest: {lowest_btc_price:.2f} {compare_prices(lowest_btc_price, yesterday_lowest_btc_price, "")} USD, highest: {highest_btc_price:.2f} USD {compare_prices(highest_btc_price, yesterday_highest_btc_price, "")}, average: {avg_btc_price:.2f} USD {compare_prices(avg_btc_price, yesterday_avg_btc_price, "")}, median: {median:.2f} USD {compare_prices(median, yesterday_median, "")}')

            except Exception as e:
                client.publish(response_topic, f'Problem retrieving BTC price ({e})')

        elif command == 'btcplin':
            try:
                cur = con.cursor()

                cur.execute('SELECT btc_price, strftime("%s", ts) FROM price ORDER BY ts DESC LIMIT 1')
                latest_btc_price, latest_epoch = cur.fetchone()

                cur.execute('SELECT btc_price, strftime("%s", ts) FROM price WHERE ts < DateTime("now", "-24 hour") ORDER BY ts DESC LIMIT 1')
                h24back_btc_price, h24back_epoch = cur.fetchone()

                ts, v_avg = predict_linear(float(h24back_btc_price), int(h24back_epoch), float(latest_btc_price), int(latest_epoch), int(latest_epoch) + 86400)

                cur.execute('SELECT btc_price, strftime("%s", ts) FROM price WHERE ts >= DateTime("now", "-24 hour") ORDER BY ts ASC')
                rows = cur.fetchall()
                median = calc_median(rows)
                ts_median = rows[0][1]

                cur.execute('SELECT btc_price, strftime("%s", ts) FROM price WHERE ts >= DateTime("now", "-48 hour") and ts < DateTime("now", "-24 hour") ORDER BY ts ASC')
                rows = cur.fetchall()
                yesterday_median = calc_median(rows)
                ts_yesterday_median = rows[0][1]

                ts, v_median = predict_linear(float(yesterday_median), int(ts_yesterday_median), float(median), int(ts_median), int(ts_median) + 86400)

                cur.close()

                client.publish(response_topic, f'In 24 hours the bitcoin price may be around {v_avg:.2f} USD (based on average), or {v_median:.2f} USD (based on median)')

            except Exception as e:
                client.publish(response_topic, f'Exception while predicting BTC price (linear): {e}, line number: {e.__traceback__.tb_lineno}')

        elif command == 'btcfb':
            t = threading.Thread(target=prophet, args=(client, response_topic), daemon=True)
            t.start()

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        client.subscribe(f'{topic_prefix}from/irc/#')

        client.subscribe(f'{topic_prefix}from/bot/command')

        client.subscribe('vanheusden/bitcoin/bitstamp_usd')

def announce_thread(client):
    while True:
        try:
            announce_commands(client)

            time.sleep(4.1)

        except Exception as e:
            print(f'Failed to announce: {e}')

client = mqtt.Client()
client.connect(mqtt_server, port=1883, keepalive=4, bind_address="")
client.on_message = on_message
client.on_connect = on_connect

t1 = threading.Thread(target=announce_thread, args=(client,))
t1.start()

client.loop_forever()
