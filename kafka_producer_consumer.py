import json
import threading
import time
import ssl

from kafka import KafkaProducer, KafkaConsumer
from pymongo import MongoClient
import paho.mqtt.client as mqtt

# MQTT configuration
MQTT_BROKER = '1fa4c66cf15c47f58b38d542f4fa54d9.s1.eu.hivemq.cloud'
MQTT_PORT = 8883
MQTT_TOPIC = 'kafkademo/v1'
MQTT_USERNAME = 'hivemq'
MQTT_PASSWORD = 'Hive@123'

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'kafka_demo_topic'

# MongoDB configuration
MONGO_URI = 'mongodb://root:MyRootPassword@localhost:27018/?authSource=admin'
MONGO_DB = 'IoT-Devices'
MONGO_COLLECTION = 'SensorValues'

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Mongo Client
mongo_client = MongoClient(MONGO_URI)
mongo_collection = mongo_client[MONGO_DB][MONGO_COLLECTION]

# MQTT Callback
def on_message(client, userdata, msg):
    payload = msg.payload.decode()
    print(f"[MQTT] Received message on {msg.topic}: {payload}")
    try:
        data = json.loads(payload)
    except json.JSONDecodeError:
        data = {"message": payload}
    producer.send(KAFKA_TOPIC, value=data)
    print(f"[Kafka] Published to topic '{KAFKA_TOPIC}': {data}")

# MQTT Client Setup
def start_mqtt_client():
    client = mqtt.Client()

    # Set username and password
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

    # Enable TLS
    client.tls_set(cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLSv1_2)

    client.on_message = on_message

    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.subscribe(MQTT_TOPIC)
    print("[MQTT] Connected and subscribed with TLS:", MQTT_TOPIC)

    client.loop_forever()

# Kafka Consumer Setup
def start_kafka_consumer():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    print("[Kafka] Started consuming...")
    for message in consumer:
        data = message.value
        print(f"[Kafka -> MongoDB] Processing: {data}")
        data['processed_at'] = time.strftime('%Y-%m-%d %H:%M:%S')
        mongo_collection.insert_one(data)
        print("[MongoDB] Inserted document")

if __name__ == '__main__':
    threading.Thread(target=start_mqtt_client).start()
    threading.Thread(target=start_kafka_consumer).start()
