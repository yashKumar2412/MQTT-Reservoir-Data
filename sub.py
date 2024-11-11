import time
import paho.mqtt.client as mqtt
import json
import matplotlib.pyplot as plt

aggregated_data ={
    "Shasta": [],
    "Sonoma": [],
    "Oroville": []
}

def on_message(client, userdata, message):
    topic = message.topic
    payload = json.loads(message.payload.decode())
    print("Received " + str(payload) + " from topic " + topic)
    if topic in aggregated_data:
        aggregated_data[topic].append(payload)

def on_connect(client, userdata, flags, code):
    if code == 0:
        print("Connected successfully to MQTT broker")
    else:
        print("Connection failed with code " + code)

def on_subscribe(client, userdata, mid, granted_qos):
    print("Subscribed successfully with QoS " + str(granted_qos[0]))

broker = "test.mosquitto.org"
port = 1883

client = mqtt.Client("Subscriber")
client.on_connect = on_connect
client.on_subscribe = on_subscribe
client.on_message = on_message

client.connect(broker, port)
client.subscribe("Shasta")
client.subscribe("Sonoma")
client.subscribe("Oroville")

print("Subscriber started. Waiting for messages.")
client.loop_start()

time.sleep(30)

client.loop_stop()
client.disconnect()

print("Daily Water Level Report:")

for reservoir, data in aggregated_data.items():
    print(reservoir + ":")
    for entry in data:
        print("Date: " + entry['Date'] + ", TAF: " + entry['TAF'])

for reservoir, data in aggregated_data.items():
    dates = [entry['Date'] for entry in data]
    taf_values = [int(entry['TAF']) for entry in data]
    
    plt.figure()
    plt.plot(dates, taf_values, marker='o', linestyle='-')
    plt.title(f"{reservoir} Reservoir TAF Over Time")
    plt.xlabel("Date")
    plt.ylabel("TAF")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig("figures/" + reservoir + ".png")