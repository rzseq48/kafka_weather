from confluent_kafka import Producer
import requests
import json
import time
import os
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Kafka topic
topic = os.getenv('KAFKA_TOPIC')
KAFKA_HOST=os.getenv('KAFKA_HOST')
KAFKA_PORT=os.getenv('KAFKA_PORT')
WEATHER_URL = os.getenv('WEATHER_API_URL')
WEATHER_API_KEY = os.getenv('WEATHER_API_KEY')


KAFKA_BOOTSTRAP_SERVERS=f'{KAFKA_HOST}:{KAFKA_PORT}'

# Kafka producer configuration
conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS
}

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

def fetch_weather_data(api_key):
    api_url = f"{WEATHER_URL}?access_key={WEATHER_API_KEY}&query=Boston"
    response = requests.get(WEATHER_URL)
    if response.status_code == 200:
        return response.json()
    else:
        print(f'Failed to fetch weather data: {response.status_code}')
        return None

def produce_weather_data(producer, api_key):
    while True:
        weather_data = fetch_weather_data(api_key)
        if weather_data:
            producer.produce(topic, json.dumps(weather_data).encode('utf-8'), callback=delivery_report)
            producer.flush()  # Ensure messages are delivered immediately (optional)
        time.sleep(300)  # Fetch data every 5 minutes

if __name__ == '__main__':
    # Create Kafka producer instance
    producer = Producer(conf)

    # Example API key for weather API
    api_key = os.getenv('WEATHER_API_KEY')

    # Start producing weather data to Kafka
    produce_weather_data(producer, api_key)

    # Wait for all messages to be delivered
    producer.flush()
