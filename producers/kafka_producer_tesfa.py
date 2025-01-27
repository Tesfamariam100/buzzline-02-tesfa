"""
kafka_producer_tesfa.py

Produce informative weather data messages and send them to a Kafka topic.
"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import sys
import time
import random
import json  # Import the json library

# Import external packages
from dotenv import load_dotenv

# Import functions from local modules (assuming they exist)
from utils.utils_producer import (
    verify_services,
    create_kafka_producer,
    create_kafka_topic,
)
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################


def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("KAFKA_TOPIC", "weather_data")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_message_interval() -> int:
    """Fetch message interval from environment or use default."""
    interval = int(os.getenv("MESSAGE_INTERVAL_SECONDS", 5))  # Increased to 5 seconds
    logger.info(f"Message interval: {interval} seconds")
    return interval


#####################################
# Weather Data Generator
#####################################


def generate_weather_data(city):
    """
    Generate a dictionary containing random weather data for a given city.

    Args:
        city (str): The city name for which to generate weather data.

    Returns:
        dict: A dictionary containing weather data keys and values.
    """

    # Define possible weather conditions and temperature ranges
    conditions = ["sunny", "cloudy", "rainy", "windy"]
    min_temps = {"sunny": 60, "cloudy": 50, "rainy": 40, "windy": 45}
    max_temps = {"sunny": 85, "cloudy": 75, "rainy": 60, "windy": 65}

    # Randomly select a condition and calculate a random temperature within the range
    condition = random.choice(conditions)
    temperature = random.randint(min_temps[condition], max_temps[condition])

    return {
        "city": city,
        "condition": condition,
        "temperature": temperature,
    }


#####################################
# Message Generator
#####################################


def generate_messages(producer, topic, interval_secs):
    """
    Generate a stream of weather data messages and send them to a Kafka topic.

    Args:
        producer (KafkaProducer): The Kafka producer instance.
        topic (str): The Kafka topic to send messages to.
        interval_secs (int): Time in seconds between sending messages.
    """

    cities = ["Toronto", "New York", "London", "Paris", "Tokyo"]  # Sample list of cities

    try:
        while True:
            # Randomly select a city and generate weather data
            city = random.choice(cities)
            weather_data = generate_weather_data(city)

            # Convert data to JSON string and send message
            message_json = json.dumps(weather_data)  # Assuming you have the `json` library
            logger.info(f"Generated weather data: {message_json}")
            producer.send(topic, value=message_json.encode())  # Encode for Kafka
            logger.info(f"Sent weather data to topic '{topic}': {message_json}")

            time.sleep(interval_secs)

    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user.")
    except Exception as e:
        logger.error(f"Error in message generation: {e}")
    finally:
        producer.close()
        logger.info("Kafka producer closed.")


#####################################
# Main Function
#####################################


def main():
    """
    Main entry point for this producer.

    - Ensures the Kafka topic exists.
    - Creates a Kafka producer using the `create_kafka_producer` utility.
    - Streams generated weather data messages to the Kafka topic.
    """
    logger.info("START producer.")
    verify_services()

    # fetch .env content
    topic = get_kafka_topic()
    interval_secs = get_message_interval()

    # Create the Kafka producer
producer = create_kafka_producer()
if not producer:
    logger.error("Failed to create Kafka producer. Exiting...")
    sys.exit(3)