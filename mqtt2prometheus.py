#!/usr/bin/env python3
#
# SPDX-FileCopyrightText: 2022 Tim Hawes
#
# SPDX-License-Identifier: MIT

import logging
import os
import queue
import time

from flask import Flask, Response, redirect
import paho.mqtt.client as mqtt


logging.basicConfig(level=logging.INFO)

MQTT_HOST = os.environ.get("MQTT_HOST", "mqtt")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
TIMEOUT = int(os.environ.get("TIMEOUT", "150"))

app = Flask(__name__)
export_data = {}


def on_connect(client, userdata, flags, rc):
    client.subscribe("#")


def on_message(client, userdata, msg):
    if msg.retain:
        return

    try:
        data = msg.payload.decode()
    except UnicodeDecodeError:
        logging.warning("parse error> {}".format(msg.payload))
        return

    logging.debug("mqtt> {} {}".format(msg.topic, msg.payload))

    if data.lower() in ["false", "f", "low", "closed", "up"]:
        v = 0
    elif data.lower() in ["true", "t", "high", "open", "down", "longpress"]:
        v = 1
    elif data.startswith("{"):
        # ignore json
        return
    else:
        try:
            v = float(data)
        except ValueError:
            logging.debug("parse error> {}".format(data))
            return

    export_data[msg.topic] = v, time.time()


@app.route("/", methods=["GET"])
def prometheus_home():
    return redirect("/metrics")


@app.route("/metrics", methods=["GET"])
def prometheus_export():
    output = []
    for topic in sorted(export_data.keys()):
        value, timestamp = export_data[topic]
        if time.time() - timestamp < TIMEOUT:
            output.append(
                f'mqtt{{host="{MQTT_HOST}",port="{MQTT_PORT}",topic="{topic}"}} {value}'
            )
        else:
            logging.debug(f"purging topic {topic}")
            del export_data[topic]
    return Response("\n".join(output) + "\n", content_type="text/plain")


m = mqtt.Client()
m.enable_logger()
m.on_connect = on_connect
m.on_message = on_message
m.connect(MQTT_HOST, MQTT_PORT)

m.loop_start()
app.run(host="0.0.0.0", port=9100)
