import logging
from extract import extract_data, get_last_date
from transform import transform_data
from load import load_data


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data/etl.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def run_etl():
    """Run the full ETL pipeline."""
    try:
        # Extract
        logger.info("Starting extraction")
        last_date = get_last_date()
        raw_data = extract_data(last_date)
        logger.info(f"Extracted {len(raw_data)} records")

        # Transform
        logger.info("Starting transformation")
        transformed_data = transform_data()
        logger.info(f"Transformed {len(transformed_data)} records")

        # Load
        logger.info("Starting loading")
        total_inserted = load_data()
        logger.info(f"Loaded {total_inserted} records")

        logger.info("ETL pipeline completed successfully")
        return total_inserted

    except Exception as e:
        logger.error(f"ETL pipeline failed: {e}")
        raise

if __name__ == "__main__":
    run_etl()