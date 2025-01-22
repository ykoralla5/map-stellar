import requests
import json
import time
import logging
import os

os.makedirs('APIcall', exist_ok=True)

logging.basicConfig(
    filename='APIcall/ledger_fetch.log', 
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)

BASE_URL = "https://horizon.stellar.org"
LEDGER_ENDPOINT = "/ledgers"

CURSOR = "51948940"
LIMIT = 200
ORDER = "asc"

LEDGER_JSON = 'APIcall/ledgers.json'
CURSOR_FILE = 'APIcall/last_cursor.txt'

def fetch_data_incremental(endpoint, cursor, limit, order, output_file):
    url = f"{BASE_URL}{endpoint}?cursor={cursor}&limit={limit}&order={order}"
    while url:
        try:
            logging.info(f"Fetching data from {url}")
            response = requests.get(url)
            if response.status_code == 429:  # if too Many Requests
                retry_after = int(response.headers.get('Retry-After', 1))
                logging.warning(f"Rate limit hit. Retrying after {retry_after} seconds...")
                time.sleep(retry_after)
                continue
            response.raise_for_status()  # exception for HTTP errors
            json_data = response.json()

            # save fetched records incrementally
            with open(output_file, mode='a') as file:
                for record in json_data['_embedded']['records']:
                    file.write(json.dumps(record) + "\n")

            logging.info(f"Fetched and saved {len(json_data['_embedded']['records'])} records.")

            # move to the next page URL
            url = json_data['_links']['next']['href'] if '_links' in json_data and 'next' in json_data['_links'] else None
            cursor = json_data['_embedded']['records'][-1]['paging_token']

            # save cursor progress to resume later if interrupted
            with open(CURSOR_FILE, 'w') as cursor_file:
                cursor_file.write(cursor)

            time.sleep(0.5)  # to avoid api call limits
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching data: {e}")
            break

def load_last_cursor(default_cursor):
    try:
        with open(CURSOR_FILE, 'r') as cursor_file:
            return cursor_file.read().strip()
    except FileNotFoundError:
        return default_cursor

logging.info("Starting incremental ledger data fetch...")
print("Fetching Ledgers Data...")
CURSOR = load_last_cursor(CURSOR)
fetch_data_incremental(LEDGER_ENDPOINT, CURSOR, LIMIT, ORDER, LEDGER_JSON)
print(f"Saved ledger data incrementally to {LEDGER_JSON}")
logging.info("Completed incremental fetching and saving of ledgers.")
