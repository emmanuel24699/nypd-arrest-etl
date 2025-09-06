import requests
import pandas as pd
import psycopg2
from datetime import datetime
import json
from dotenv import load_dotenv
import os
import logging
import time
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data/extract.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Constants
API_URL = "https://data.cityofnewyork.us/resource/8h9b-rp9u.json"
BATCH_SIZE = 50000  # SODA 2.0 max limit
OUTPUT_PATH = "data/raw_data.json"
CHECKPOINT_PATH = "data/extract_checkpoint.json"

def get_db_connection():
    """Establish database connection using DATABASE_URL from .env."""
    try:
        load_dotenv()
        conn = psycopg2.connect(
            os.getenv("DATABASE_URL"),
            sslmode="require"
        )
        logger.info("Database connection established.")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        raise

def get_last_date():
    """Retrieve the latest arrest_date from the database."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT MAX(arrest_date) FROM nypd_arrests;")
        result = cur.fetchone()[0]
        conn.close()
        if result:
            logger.info(f"Latest arrest_date in database: {result}")
            return result
        logger.info("No arrest_date found in database; performing full extract.")
        return None
    except Exception as e:
        logger.error(f"Error querying last arrest_date: {e}")
        return None

@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=4, max=20),
    retry=retry_if_exception_type(requests.exceptions.RequestException)
)
def fetch_batch(params):
    """Fetch a single batch from the API with retry logic."""
    try:
        response = requests.get(API_URL, params=params, timeout=15)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.error(f"API request failed: {e}")
        raise

def save_checkpoint(offset):
    """Save checkpoint with current offset."""
    try:
        checkpoint = {"last_offset": offset}
        os.makedirs('data', exist_ok=True)
        with open(CHECKPOINT_PATH, 'w') as f:
            json.dump(checkpoint, f)
        logger.info(f"Saved checkpoint at offset {offset}")
    except Exception as e:
        logger.error(f"Failed to save checkpoint: {e}")
        raise

def load_checkpoint():
    """Load checkpoint if exists."""
    if os.path.exists(CHECKPOINT_PATH):
        try:
            with open(CHECKPOINT_PATH, 'r') as f:
                checkpoint = json.load(f)
            logger.info(f"Loaded checkpoint: last_offset={checkpoint['last_offset']}")
            return checkpoint['last_offset'], []
        except Exception as e:
            logger.warning(f"Failed to load checkpoint: {e}; starting fresh")
    return 0, []

def extract_data(last_date=None):
    """
    Extract data from NYC Open Data API using SODA 2.0.
    If last_date is provided, fetch records after that date.
    """
    # Load checkpoint or start fresh
    offset, data = load_checkpoint()
    
    params = {
        '$limit': BATCH_SIZE,
        '$order': 'arrest_date ASC'
    }

    if last_date:
        params['$where'] = f"arrest_date > '{last_date.strftime('%Y-%m-%d')}'"

    # Initialize JSON Lines file
    os.makedirs('data', exist_ok=True)
    if offset == 0:
        with open(OUTPUT_PATH, 'w') as f:
            f.write('')  # Clear file for fresh start

    while True:
        params['$offset'] = offset
        try:
            logger.info(f"Fetching batch: offset={offset}")
            batch = fetch_batch(params)
            if not batch:
                logger.info("No more data to fetch.")
                break

            # Validate batch
            df = pd.DataFrame(batch)
            if 'arrest_key' not in df.columns or 'arrest_date' not in df.columns:
                missing = [col for col in ['arrest_key', 'arrest_date'] if col not in df.columns]
                logger.error(f"Missing required columns {missing} in batch at offset {offset}")
                raise ValueError(f"Missing required columns: {missing}")
            logger.info(f"Batch columns: {list(df.columns)}")

            data.extend(batch)
            offset += BATCH_SIZE
            logger.info(f"Fetched {len(batch)} records, total so far: {len(data)}")

            # Append batch to JSON Lines file
            try:
                with open(OUTPUT_PATH, 'a') as f:
                    df.to_json(f, orient='records', lines=True, index=False)
                logger.info(f"Appended {len(batch)} records to {OUTPUT_PATH}")
                save_checkpoint(offset)
            except Exception as e:
                logger.error(f"Failed to save batch: {e}")
                raise

            # Delay to avoid overwhelming API
            time.sleep(2)

        except requests.RequestException as e:
            logger.error(f"Retry failed for batch at offset {offset}: {e}")
            raise

    # Clear checkpoint after successful completion
    if os.path.exists(CHECKPOINT_PATH):
        os.remove(CHECKPOINT_PATH)
        logger.info("Cleared checkpoint after successful extraction")

    return data

def main():
    """Main function to run the extraction process."""
    try:
        last_date = get_last_date()
        data = extract_data(last_date)
        logger.info(f"Extraction complete. Total records: {len(data)}")
        return data
    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        raise

if __name__ == "__main__":
    main()