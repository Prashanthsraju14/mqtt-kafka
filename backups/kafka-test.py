from kafka import KafkaProducer
import time

# Use the Kubernetes service DNS name for your Kafka bootstrap
bootstrap_servers = 'my-cluster-kafka-bootstrap:9092'

topic = 'my-test-topic'

producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

try:
    i = 0
    while True:
        message = f"Hello Kafka {i}"
        producer.send(topic, value=message.encode('utf-8'))
        print(f"Sent: {message}")
        i += 1
        time.sleep(5)  # Send a message every 5 seconds
except KeyboardInterrupt:
    print("Stopping producer...")

finally:
    producer.flush()
    producer.close()
