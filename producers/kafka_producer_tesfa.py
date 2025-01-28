"""
kafka_producer_tesfa.py

Produce informative weather data messages and send them to a Kafka topic.
"""

import os
import sys
import time
import random
import json
from dotenv import load_dotenv

from utils.logger import logger  # Import logger from the 'utils' package

def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("KAFKA_TOPIC", "weather_data")
    logger.info(f"Kafka topic: {topic}")
    return topic

def get_message_interval() -> int:
    """Fetch message interval from environment or use default."""
    interval = int(os.getenv("MESSAGE_INTERVAL_SECONDS", 5))
    logger.info(f"Message interval: {interval} seconds")
    return interval

def generate_weather_data(city):
    """
    Generate a dictionary containing random weather data for a given city.

    Args:
        city (str): The city name for which to generate weather data.

    Returns:
        dict: A dictionary containing weather data keys and values.
    """

    conditions = ["sunny", "cloudy", "rainy", "windy"]
    min_temps = {"sunny": 60, "cloudy": 50, "rainy": 40, "windy": 45}
    max_temps = {"sunny": 85, "cloudy": 75, "rainy": 60, "windy": 65}

    condition = random.choice(conditions)
    temperature = random.randint(min_temps[condition], max_temps[condition])

    return {
        "city": city,
        "condition": condition,
        "temperature": temperature,
    }

def generate_messages(producer, topic, interval_secs):
    """
    Generate a stream of weather data messages and send them to a Kafka topic.

    Args:
        producer (KafkaProducer): The Kafka producer instance.
        topic (str): The Kafka topic to send messages to.
        interval_secs (int): Time in seconds between sending messages.
    """

    cities = ["Toronto", "New York", "London", "Paris", "Tokyo"]

    try:
        while True:
            city = random.choice(cities)
            weather_data = generate_weather_data(city)
            message_json = json.dumps(weather_data)
            logger.info(f"Generated weather data: {message_json}")
            producer.send(topic, value=message_json.encode())
            logger.info(f"Sent weather data to topic '{topic}': {message_json}")

            time.sleep(interval_secs)

    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user.")
    except Exception as e:
        logger.error(f"Error in message generation: {e}")
    finally:
        producer.close()
        logger.info("Kafka producer closed.")

def main():
    """
    Main entry point for this producer.

    - Ensures the Kafka topic exists.
    - Creates a Kafka producer using the `create_kafka_producer` utility.
    - Streams generated weather data messages to the Kafka topic.
    """
    logger.info("START producer.")
    verify_services()

    topic = get_kafka_topic()
    interval_secs = get_message_interval()

    producer = create_kafka_producer()
    if not producer:
        logger.error("Failed to create Kafka producer. Exiting...")
        sys.exit(3)

    generate_messages(producer, topic, interval_secs)

if __name__ == "__main__":
    main()