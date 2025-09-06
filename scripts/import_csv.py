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
CHUNK_SIZE = 50000

def import_csv():
    """Import CSV data and save as JSON Lines."""
    try:
        if not os.path.exists(CSV_PATH):
            logger.error(f"CSV file {CSV_PATH} does not exist")
            raise FileNotFoundError(f"{CSV_PATH} not found")

        os.makedirs('data', exist_ok=True)
        with open(OUTPUT_PATH, 'w') as f:
            f.write('')

        total_records = 0
        for chunk in pd.read_csv(CSV_PATH, chunksize=CHUNK_SIZE, low_memory=False):
            logger.info(f"Processing chunk: {len(chunk)} records")
            logger.info(f"Chunk columns: {list(chunk.columns)}")

            required_columns = ['arrest_key', 'arrest_date']
            missing_columns = [col for col in required_columns if col not in chunk.columns]
            if missing_columns:
                logger.error(f"Missing required columns: {missing_columns}")
                raise ValueError(f"Missing columns: {missing_columns}")

            with open(OUTPUT_PATH, 'a') as f:
                chunk.to_json(f, orient='records', lines=True, index=False)
            total_records += len(chunk)
            logger.info(f"Appended {len(chunk)} records to {OUTPUT_PATH}, total: {total_records}")

        logger.info(f"CSV import complete. Total records: {total_records}")
        return [{'total_records': total_records}]

    except Exception as e:
        logger.error(f"CSV import failed: {e}")
        raise

def main():
    """Main function to run the CSV import process."""
    try:
        imported_data = import_csv()
        logger.info(f"CSV import complete. Total records: {imported_data[0]['total_records']}")
        return imported_data
    except Exception as e:
        logger.error(f"CSV import failed: {e}")
        raise

if __name__ == "__main__":
    main()