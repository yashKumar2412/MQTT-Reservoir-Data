import paho.mqtt.client as mqtt
import json
import time
import csv

reservoir_data = {}
files = ["Shasta.csv", "Sonoma.csv", "Oroville.csv"]

for filename in files:
    with open("data/" + filename, 'r') as file:
        csv_reader = csv.DictReader(file)
        index = filename.replace(".csv", "")
        reservoir_data[index] = []

        for row in csv_reader:
            reservoir_data[index].append(row)

def on_connect(client, userdata, flags, code):
    if code == 0:
        print("Connected successfully to MQTT broker")
    else:
        print("Connection failed with code " + code)

broker = "test.mosquitto.org"
port = 1883

client = mqtt.Client("Publisher")
client.on_connect = on_connect

client.connect(broker, port)

for topic, data in reservoir_data.items():
    for entry in data:
        payload = json.dumps(entry)
        client.publish(topic, payload)
        print("Published " + payload + " to topic " + topic)
        time.sleep(1)

client.disconnect()