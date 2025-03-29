#!/usr/bin/env python3

from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import sys
import paho.mqtt.client as mqtt
import random

class Mqtt2HttpRequestHandler(BaseHTTPRequestHandler):
    def __init__(self, config):
        self.config = config

    def __call__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __del__(self):
        self.mqttc.loop_stop()
        
    def setup(self):
        BaseHTTPRequestHandler.setup(self)
        self.request.settimeout(5)
        self.topic_cache = {}

        mqtt_config = config["mqtt"]
        self.mqttc = mqtt.Client(client_id=f"mqtt2http-{random.randint(0, 1000)}")
        self.mqttc.on_connect = self.on_mqtt_connect
        self.mqttc.on_message = self.on_mqtt_message
        if "user" in mqtt_config:
            self.mqttc.username_pw_set(username=mqtt_config["user"], password=mqtt_config["password"])
        self.mqttc.connect(mqtt_config["host"], mqtt_config["port"])
        self.mqttc.loop_start()

    def do_GET(self):
        payload = self.topic_cache.get(self.path, None)
        if payload is not None:
            self.send_response(200)
            self.end_headers()
            self.wfile.write(payload)
        else:
            self.send_error(404)

    def do_POST(self):
        topic = self.path.removeprefix("/")
        length = int(self.headers.get("content-length"))
        payload = self.rfile.read(length)
        if topic and payload:
            self.mqttc.publish(topic, payload)
            self.send_response(200)
            self.end_headers()
        else:
            self.send_error(404)

    def on_mqtt_connect(self, client, userdata, flags, reason_code):
        for topic in self.config["mqtt"]["topics"]:
            client.subscribe(topic)

    def on_mqtt_message(self, client, userdata, msg):
        self.topic_cache["/" + msg.topic] = msg.payload


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: mqtt2http <config.json>")
        exit(1)

    config = {}
    with open(sys.argv[1], "r") as f:
        config = json.load(f)

    http_config = config["http"]
    handler = Mqtt2HttpRequestHandler(config)
    server = HTTPServer((http_config["address"], http_config["port"]), handler)
    server.timeout = 10
    server.serve_forever()
