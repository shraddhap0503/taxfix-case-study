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

    # Calculate the number of requests needed
    requests_needed = (quantity // 1000) + (1 if quantity % 1000 != 0 else 0)

    total_data = []  # To store all the fetched records
    for i in range(requests_needed):
        # Adjust the quantity per request if it's the last one
        current_quantity = 1000 if i < requests_needed - 1 else (quantity - 1000 * i)
        url = api_url.format(quantity=current_quantity, gender=gender, birthday_start=birthday_start)

        attempt = 0
        while attempt < retries:
            try:
                logging.info(f"Fetching request {i + 1}/{requests_needed} (Quantity: {current_quantity})...")

                # Send the request
                response = requests.get(url, timeout=10)

                if response.status_code == 200:
                    logging.info(f"Request {i + 1} fetched successfully.")
                    total_data.extend(response.json().get("data", []))
                    break
                elif response.status_code >= 500:
                    logging.warning(f"Internal Server Error (Request {i + 1}): Retrying...")
                    attempt += 1
                    if attempt < retries:
                        logging.info(f"Retrying {i + 1} in {delay} seconds...")
                        time.sleep(delay)
                    else:
                        logging.error(f"Failed to fetch request {i + 1} after {retries} attempts.")
                        break
                else:
                    raise Exception(f"Unexpected status code: {response.status_code} - {response.text}")

            except requests.exceptions.RequestException as e:
                logging.warning(f"Attempt {attempt + 1}/{retries} failed for request {i + 1}: {e}")
                attempt += 1
                if attempt < retries:
                    logging.info(f"Retrying {i + 1} in {delay} seconds...")
                    time.sleep(delay)
                else:
                    logging.error(f"Failed to fetch request {i + 1} after {retries} attempts.")
                    break

        # Stop if the desired quantity of data has been fetched
        if len(total_data) >= quantity:
            break

    return total_data


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
