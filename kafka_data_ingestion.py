from confluent_kafka import Producer
import requests
import json
import time

# Kafka broker address and port (default is localhost:9092)
topic = 'weather_data_topic'

# Kafka producer configuration
conf = {'bootstrap.servers': 'localhost:9092'}

########################################################################

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

# Function to fetch data from weather API
def fetch_weather_data(api_key):
    url = f'http://api.weatherstack.com/current?access_key={api_key}&query= boston'
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f'Failed to fetch weather data: {response.status_code}')
        return None

# Main function to produce data to Kafka
def produce_weather_data(producer, api_key):
    """
    1. Fetching the data from weather api using fetch_weather_data function
    2. Ingest the data from API source using the produce function 
    """
    while True:
        weather_data = fetch_weather_data(api_key)
        if weather_data:
            producer.produce(topic, json.dumps(weather_data).encode('utf-8'), callback=delivery_report)
        time.sleep(300)  # Fetch data every 5 minutes


if __name__ == '__main__':
    # Create Kafka producer instance
    producer = Producer(conf)

    # Example API key for OpenWeatherMap (replace with your own)
    api_key = 'f0b63c15afb6890c42cce7254986db0f'

    # Start producing weather data to Kafka
    produce_weather_data(producer, api_key)

    # Wait for all messages to be delivered
    producer.flush()
