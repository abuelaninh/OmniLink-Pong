import paho.mqtt.client as mqtt
import json
import time

client = mqtt.Client(transport="websockets")
client.connect("localhost", 9001, 60)
client.loop_start()

# Test pause
client.publish("olink/commands", json.dumps({"command": "pause_game"}))
print("Published pause_game")

time.sleep(1)
client.loop_stop()
client.disconnect()
