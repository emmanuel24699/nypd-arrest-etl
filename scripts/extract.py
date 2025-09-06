import requests
import pandas as pd
import logging
import os
import json
from tenacity import retry, stop_after_attempt, wait_exponential
from datetime import datetime
from dotenv import load_dotenv

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
BASE_URL = "https://data.cityofnewyork.us/resource/8h9b-rp9u.json"
OUTPUT_PATH = "data/raw_data.json"
CHECKPOINT_PATH = "data/extract_checkpoint.json"
BATCH_SIZE = 50000

def get_db_connection():
    """Establish database connection using DATABASE_URL from .env."""
    try:
        import psycopg2
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

def get_latest_arrest_date():
    """Retrieve the latest arrest_date from the database."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT MAX(arrest_date) FROM nypd_arrests")
        latest_date = cur.fetchone()[0]
        conn.close()
        logger.info(f"Latest arrest_date in database: {latest_date}")
        return latest_date if latest_date else '1900-01-01'  # Default to a very early date
    except Exception as e:
        logger.error(f"Failed to retrieve latest arrest_date: {e}")
        return '1900-01-01'

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def fetch_batch(offset, latest_date):
    """Fetch a batch of data from the API."""
    try:
        params = {
            '$limit': BATCH_SIZE,
            '$offset': offset,
            '$where': f"arrest_date > '{latest_date}'"
        }
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        data = response.json()
        logger.info(f"Fetched batch: offset={offset}, records={len(data)}")
        return data
    except Exception as e:
        logger.error(f"Failed to fetch batch at offset {offset}: {e}")
        raise

def save_checkpoint(total_records, offset):
    """Save extraction progress to checkpoint file."""
    try:
        with open(CHECKPOINT_PATH, 'w') as f:
            json.dump({'total_records': total_records, 'offset': offset}, f)
        logger.info(f"Saved checkpoint: total_records={total_records}, offset={offset}")
    except Exception as e:
        logger.error(f"Failed to save checkpoint: {e}")
        raise

def load_checkpoint():
    """Load extraction progress from checkpoint file."""
    try:
        if os.path.exists(CHECKPOINT_PATH):
            with open(CHECKPOINT_PATH, 'r') as f:
                checkpoint = json.load(f)
                logger.info(f"Loaded checkpoint: {checkpoint}")
                return checkpoint.get('total_records', 0), checkpoint.get('offset', 0)
        return 0, 0
    except Exception as e:
        logger.error(f"Failed to load checkpoint: {e}")
        return 0, 0

def extract_data():
    """Extract data from API and save as JSON Lines."""
    try:
        total_records, offset = load_checkpoint()
        os.makedirs('data', exist_ok=True)

        latest_date = get_latest_arrest_date()

        with open(OUTPUT_PATH, 'w' if offset == 0 else 'a') as f:
            if offset == 0:
                f.write('')

        while True:
            data = fetch_batch(offset, latest_date)
            if not data:
                logger.info("No more data to fetch.")
                break

            df = pd.DataFrame(data)
            logger.info(f"Batch columns: {list(df.columns)}")

            required_columns = ['arrest_key', 'arrest_date']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                logger.error(f"Missing required columns: {missing_columns}")
                raise ValueError(f"Missing columns: {missing_columns}")

            with open(OUTPUT_PATH, 'a') as f:
                df.to_json(f, orient='records', lines=True, index=False)
            total_records += len(df)
            offset += BATCH_SIZE
            logger.info(f"Appended {len(df)} records to {OUTPUT_PATH}, total: {total_records}")

            save_checkpoint(total_records, offset)

            if len(data) < BATCH_SIZE:
                logger.info("Reached end of data.")
                break

        logger.info(f"Extraction complete. Total records: {total_records}")
        return [{'total_records': total_records}]

    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        raise

def main():
    """Main function to run the extraction process."""
    try:
        extracted_data = extract_data()
        logger.info(f"Extraction complete. Total records: {extracted_data[0]['total_records']}")
        return extracted_data
    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        raise

if __name__ == "__main__":
    main()