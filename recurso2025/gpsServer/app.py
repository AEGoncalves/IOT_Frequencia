import json
import time
import paho.mqtt.client as mqtt

id = '20171165'

client_telemetry_topic = id + '/telemetryrecurso'
server_command_topic   = id + '/commandrecurso'
client_name = id + '_servidor'

def on_message(client, userdata, msg):
    payload = json.loads(msg.payload.decode())
    print("Recebido:", payload)

    led_state = payload['longitude'] > 0   # TRUE se longitude > 0

    command = {'led_on': led_state}
    print("A enviar comando:", command)

    client.publish(server_command_topic, json.dumps(command))

mqtt_client = mqtt.Client(client_name)
mqtt_client.connect('test.mosquitto.org')

mqtt_client.subscribe(client_telemetry_topic)
mqtt_client.on_message = on_message

mqtt_client.loop_forever()
