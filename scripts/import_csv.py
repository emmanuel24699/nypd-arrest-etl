import pandas as pd
import logging
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data/import_csv.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Constants
CSV_PATH = "data/nypd_arrests_historic.csv"
OUTPUT_PATH = "data/raw_data.json"
CHUNK_SIZE = 100000

def import_csv():
    """Import NYPD Arrest Data from CSV and save as JSON Lines."""
    try:
        if not os.path.exists(CSV_PATH):
            logger.error(f"CSV file {CSV_PATH} does not exist")
            raise FileNotFoundError(f"{CSV_PATH} not found")

        data = []
        logger.info(f"Reading CSV from {CSV_PATH}")
        for chunk in pd.read_csv(CSV_PATH, chunksize=CHUNK_SIZE):
            # Convert chunk to list of dicts
            chunk_data = chunk.to_dict('records')
            data.extend(chunk_data)
            logger.info(f"Processed chunk: {len(chunk)} records, total so far: {len(data)}")

        # Save as JSON Lines
        try:
            os.makedirs('data', exist_ok=True)
            pd.DataFrame(data).to_json(OUTPUT_PATH, orient='records', lines=True)
            logger.info(f"Saved {len(data)} records to {OUTPUT_PATH}")
        except Exception as e:
            logger.error(f"Failed to save JSON: {e}")
            raise

        return data

    except Exception as e:
        logger.error(f"CSV import failed: {e}")
        raise

def main():
    """Main function to run the CSV import process."""
    try:
        data = import_csv()
        logger.info(f"CSV import complete. Total records: {len(data)}")
        return data
    except Exception as e:
        logger.error(f"CSV import failed: {e}")
        raise

if __name__ == "__main__":
    main()