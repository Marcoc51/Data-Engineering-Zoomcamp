import json
from kafka import KafkaProducer
import pandas as pd
from time import time

# Start tracking execution time
t0 = time()

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

# Kafka broker address
server = 'localhost:9092'
topic_name = 'green-trips'

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=json_serializer
)

print("-----------------------------------")
print('Connected to Kafka')
print(producer.bootstrap_connected())

# Define the file path
file_path = "7. Week 6 - Streaming Processing - Flink\Data\green_tripdata_2019-10.csv"

# Define the columns to keep
columns_to_keep = [
    'lpep_pickup_datetime',
    'lpep_dropoff_datetime',
    'PULocationID',
    'DOLocationID',
    'passenger_count',
    'trip_distance',
    'tip_amount'
]

# Read the CSV file with only the necessary columns
df = pd.read_csv(file_path, usecols=columns_to_keep)

# Display the first few rows
print("-----------------------------------")
print("Sample Data:")
print(df.head())

# Send each row as a message to the Kafka topic
print("-----------------------------------")
print(f"Sending data to Kafka topic: {topic_name}")

try:
    for _, row in df.iterrows():
        message = row.to_dict()  # Convert row to dictionary
        producer.send(topic_name, value=message)  # Send message to Kafka

    print("-----------------------------------")
    print("All messages successfully sent!")

except Exception as e:
    print("-----------------------------------")
    print(f"Error while sending messages: {e}")

# Ensure all messages are delivered before closing the producer
producer.flush()
print("-----------------------------------")
print("Producer flush completed.")

# Stop execution time tracking
t1 = time()
took = t1 - t0
print("-----------------------------------")
print(f"Execution completed in {took:.2f} seconds.")
