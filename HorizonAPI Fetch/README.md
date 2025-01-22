# Stellar Ledger Data Fetcher

## Overview
This script fetches ledger data from the Stellar Horizon API incrementally, ensuring that previously fetched data is not duplicated. It logs all activity, handles rate limits, and maintains a persistent cursor to allow for resumption in case of interruptions.

## Features
- Fetches ledger data from the [Stellar Horizon API](https://developers.stellar.org/docs/glossary/horizon/).
- Handles API rate limits with automatic retries.
- Saves fetched records incrementally to a JSON file.
- Maintains a cursor to resume fetching from the last retrieved ledger automatically.
- Logs all actions in a dedicated log file.

## Prerequisites
Ensure you have Python installed along with the following dependencies:

```bash
pip install requests
```

## Usage
### 1. Clone the Repository
```bash
git clone <repository-url>
cd <repository-folder>
```

### 2. Run the Script
```bash
python fetch_ledgers.py
```

### 3. Fetching Process
- The script starts fetching ledgers from the last saved cursor in `APIcall/last_cursor.txt`. If the file does not exist, it uses the default cursor.
- It saves results incrementally in `APIcall/ledgers.json`.
- It updates the last retrieved cursor in `APIcall/last_cursor.txt`.

## File Structure
```
.
├── fetch_ledgers.py  # Main script
├── APIcall/
│   ├── ledgers.json  # Output file containing fetched ledgers
│   ├── last_cursor.txt  # Stores last cursor position for resumption
│   ├── ledger_fetch.log  # Log file for tracking fetch activity
```

## Configuration
Modify the following variables in `fetch_ledgers.py` if necessary:

```python
BASE_URL = "https://horizon.stellar.org"
LEDGER_ENDPOINT = "/ledgers"
LIMIT = 200  # Number of records per request
ORDER = "asc"  # Fetch order
```

## Automatic Cursor Management
- The script automatically retrieves the last processed cursor from `APIcall/last_cursor.txt`.
- If the file does not exist, it starts fetching from the default cursor value.
- The cursor is updated dynamically as new data is retrieved.

## Handling API Rate Limits
- The script automatically waits before retrying when encountering rate limits (`HTTP 429`).
- By default, it retries after the time specified in the `Retry-After` header.

## Error Handling
- Logs API errors and exceptions to `APIcall/ledger_fetch.log`.
- If an error occurs, the script stops execution but preserves the last cursor for later resumption.

## License
This project is open-source and available under the MIT License.


