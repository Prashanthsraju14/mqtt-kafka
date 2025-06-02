import ssl
import paho.mqtt.client as mqtt
from datetime import datetime

# MQTT configuration
MQTT_BROKER = '1fa4c66cf15c47f58b38d542f4fa54d9.s1.eu.hivemq.cloud'
MQTT_PORT = 8883
MQTT_TOPIC = 'kafkademo/v1'
MQTT_USERNAME = 'hivemq'
MQTT_PASSWORD = 'Hive@123'

# Callback when connected to broker
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("✅ Connected to MQTT broker")
        client.subscribe(MQTT_TOPIC)
        print(f"🔔 Subscribed to topic: {MQTT_TOPIC}")
    else:
        print(f"❌ Failed to connect, return code {rc}")

# Callback when a message is received
def on_message(client, userdata, msg):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"\n📥 [{timestamp}] Data received:")
    print(f"    • Topic: {msg.topic}")
    print(f"    • Payload: {msg.payload.decode()}")

def main():
    client = mqtt.Client()
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

    # Enable TLS
    client.tls_set(tls_version=ssl.PROTOCOL_TLSv1_2)
    client.tls_insecure_set(False)

    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    client.loop_forever()

if __name__ == "__main__":
    main()
