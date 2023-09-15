from confluent_kafka import Producer

# Kafka broker configuration
bootstrap_servers = 'localhost:9092'  # Replace with your Kafka broker address
topic = 'test123'

# Create a Kafka producer instance
producer = Producer({'bootstrap.servers': bootstrap_servers})

# Produce 1000 messages to the Kafka topic
for i in range(1, 1001):
    message = f"Message {i}"
    producer.produce(topic, key=str(i), value=message)
    print(f"Produced: {message}")

# Wait for any outstanding messages to be delivered and delivery reports received
producer.flush()
