"""
kafka_consumer_weather_tesfa.py

Consume weather data messages from a Kafka topic and process them.
"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import json  # Assuming you have the `json` library

# Import external packages
from dotenv import load_dotenv

# Import functions from local modules
from utils.utils_consumer import create_kafka_consumer
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
    topic = os.getenv("KAFKA_TOPIC", "weather_data")  # Assuming the producer uses this topic
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_kafka_consumer_group_id() -> int:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id: str = os.getenv("KAFKA_CONSUMER_GROUP_ID", "weather_data_consumers")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id


#####################################
# Define a function to process a weather data message
#####################################


def process_weather_data(message_json: str) -> None:
    """
    Process a weather data message in JSON format.

    Parses the JSON message, extracts the city, condition, and temperature,
    and logs the information.

    Args:
        message_json (str): The weather data message in JSON format.
    """

    try:
        # Assuming the message is a JSON-encoded dictionary
        data = json.loads(message_json)
        city = data["city"]
        condition = data["condition"]
        temperature = data["temperature"]

        logger.info(
            f"Weather update: City: {city}, Condition: {condition}, Temperature: {temperature}°C"
        )
    except json.JSONDecodeError:
        logger.error(f"Error decoding JSON message: {message_json}")
    except KeyError as e:
        logger.error(f"Missing key in JSON data: {e}")


#####################################
# Define main function for this module
#####################################


def main() -> None:
    """
    Main entry point for the consumer.

    - Reads the Kafka topic name and consumer group ID from environment variables.
    - Creates a Kafka consumer using the `create_kafka_consumer` utility.
    - Processes weather data messages from the Kafka topic.
    """
    logger.info("START consumer.")

    # fetch .env content
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}'...")

    # Create the Kafka consumer using the helpful utility function.
    consumer = create_kafka_consumer(topic, group_id)

    # Poll and process messages
    logger.info(f"Polling messages from topic '{topic}'...")
    try:
        for message in consumer:
            message_str = message.value.decode()  # Decode from bytes
            logger.debug(f"Received message at offset {message.offset}: {message_str}")
            process_weather_data(message_str)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed.")

    logger.info(f"END consumer for topic '{topic}' and group '{group_id}'.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()