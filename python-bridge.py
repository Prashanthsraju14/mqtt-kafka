import ssl
import json
import pika
import pymongo
from paho.mqtt import client as mqtt_client

# MQTT config
MQTT_BROKER = '1fa4c66cf15c47f58b38d542f4fa54d9.s1.eu.hivemq.cloud'
MQTT_PORT = 8883
MQTT_TOPIC = 'demo/v1'
MQTT_USERNAME = 'hivemq'
MQTT_PASSWORD = 'Hive@123'

# RabbitMQ config
RABBITMQ_HOST = 'localhost'  # your RabbitMQ IP or hostname
RABBITMQ_QUEUE = 'mqtt_messages'
RABBITMQ_USERNAME = 'demo'
RABBITMQ_PASSWORD = 'demo'

# MongoDB config
MONGODB_URI = 'mongodb://root:MyRootPassword@localhost:27018/admin'
MONGODB_DB = 'IoT-Devices'
MONGODB_COLLECTION = 'SensorValues'

# Setup MongoDB client
mongo_client = pymongo.MongoClient(MONGODB_URI)
mongo_db = mongo_client[MONGODB_DB]
mongo_collection = mongo_db[MONGODB_COLLECTION]

# Setup RabbitMQ connection and channel with credentials
credentials = pika.PlainCredentials(RABBITMQ_USERNAME, RABBITMQ_PASSWORD)
rabbitmq_connection = pika.BlockingConnection(
    pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)
)
rabbitmq_channel = rabbitmq_connection.channel()
rabbitmq_channel.queue_declare(queue=RABBITMQ_QUEUE)  # idempotent queue create

# RabbitMQ consumer callback to insert into MongoDB
def callback(ch, method, properties, body):
    print("Received in RabbitMQ:", body)
    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        data = {"message": body.decode()}
    mongo_collection.insert_one(data)
    ch.basic_ack(delivery_tag=method.delivery_tag)

# Start RabbitMQ consumer
def start_rabbitmq_consumer():
    rabbitmq_channel.basic_consume(queue=RABBITMQ_QUEUE, on_message_callback=callback)
    print("Starting RabbitMQ consumer...")
    rabbitmq_channel.start_consuming()

# MQTT message callback - publish to RabbitMQ queue
def on_message(client, userdata, msg):
    print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
    rabbitmq_channel.basic_publish(exchange='', routing_key=RABBITMQ_QUEUE, body=msg.payload)

def connect_mqtt():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
            client.subscribe(MQTT_TOPIC)
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client()
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    client.tls_set(tls_version=ssl.PROTOCOL_TLS)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_BROKER, MQTT_PORT)
    return client

if __name__ == '__main__':
    import threading

    # Run RabbitMQ consumer in a background thread
    consumer_thread = threading.Thread(target=start_rabbitmq_consumer, daemon=True)
    consumer_thread.start()

    # Connect MQTT and loop forever
    mqtt_client_instance = connect_mqtt()
    mqtt_client_instance.loop_forever()