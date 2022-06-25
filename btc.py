#! /usr/bin/python3

# by FvH, released under Apache License v2.0

# either install 'python3-paho-mqtt' or 'pip3 install paho-mqtt'

import paho.mqtt.client as mqtt
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

def on_message(client, userdata, message):
    global prefix

    text = message.payload.decode('utf-8')

    topic = message.topic[len(topic_prefix):]

    print(message.topic, text)

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

    if channel in channels:
        response_topic = f'{topic_prefix}to/irc/{channel}/privmsg'

        tokens  = text.split(' ')

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
