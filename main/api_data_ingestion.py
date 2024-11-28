import json
import requests
import time
from datetime import datetime
import logging


# Configure logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# Load configuration
try:
    with open("./config/config.json", "r") as config_file:
        config = json.load(config_file)
except FileNotFoundError:
    logging.error("Configuration file 'config.json' not found.")
    raise


# Function to fetch data using Faker API
def fetch_data(api_url, quantity, gender, birthday_start, retries=3, delay=2):
    """
    Fetch data from the Faker API with retry logic.

    :param api_url: The Faker API URL template
    :param quantity: Number of records to fetch
    :param gender: Gender filter
    :param birthday_start: Start date for birthday
    :param retries: Number of retry attempts
    :param delay: Delay between retries in seconds
    :return: Fetched data
    """
    total_data = []  # To store all the fetched records
    # Calculate the number of requests needed
    requests_needed = (quantity // 1000) + (1 if quantity % 1000 != 0 else 0)

    for attempt in range(retries):
        try:
            logging.info(f"Fetching {quantity} records in {requests_needed} requests...")
            for i in range(requests_needed):
                # Adjust the quantity per request if it's the last one
                current_quantity = 1000 if i < requests_needed - 1 else (quantity - 1000 * i)
                url = api_url.format(quantity=current_quantity, gender=gender, birthday_start=birthday_start)

                logging.info(f"Fetching request {i + 1}/{requests_needed} (Quantity: {current_quantity})...")
                response = requests.get(url, timeout=10)

                if response.status_code == 200:
                    logging.info(f"Request {i + 1} fetched successfully.")
                    total_data.extend(response.json().get("data", []))  # Add new records to the total data
                elif response.status_code >= 500:
                    logging.warning(f"Internal Server Error (Request {i + 1}): Retrying...")
                    break
                else:
                    raise Exception(f"Unexpected status code: {response.status_code} - {response.text}")

                if len(total_data) >= quantity:
                    break

            return total_data

        except requests.exceptions.RequestException as e:
            logging.warning(f"Attempt {attempt + 1}/{retries} failed: {e}")
            if attempt < retries - 1:
                logging.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                logging.error(f"Failed to fetch data after {retries} attempts.")
                raise


# Main logic to fetch and save data
def main():
    try:
        api_url = config["faker_api_url"]
        quantity = config["default_quantity"]
        gender = config["default_gender"]
        birthday_start = config["birthday_start"]
        output_raw_path = config["output_raw_path"]
    except KeyError as e:
        logging.error(f"Missing configuration key: {e}")
        raise

    logging.info("Starting data fetch process...")

    try:
        # Fetch data
        data = fetch_data(api_url, quantity, gender, birthday_start)

        # Save data to a JSON file
        output_file = f"{output_raw_path}/person_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(output_file, "w") as f:
            json.dump(data, f)

        logging.info(f"Data successfully fetched and saved to {output_file}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        raise


if __name__ == "__main__":
    main()
