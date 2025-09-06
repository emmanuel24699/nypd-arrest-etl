import logging
from extract import extract_data, get_latest_arrest_date
from transform import transform_data
from load import load_data
from import_csv import import_csv
from dotenv import load_dotenv
import os

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
        # Load environment variables
        load_dotenv()
        use_csv = os.getenv('USE_CSV', 'false').lower() == 'true'
        logger.info(f"Starting ETL pipeline with USE_CSV={use_csv}")

        # Step 1: Extract or import data
        if use_csv:
            logger.info("Running CSV import")
            import_result = import_csv()
            logger.info(f"CSV import result: {import_result}")
        else:
            logger.info("Running data extraction")
            extract_result = extract_data()
            logger.info(f"Extraction result: {extract_result}")

        # Step 2: Transform data
        logger.info("Running data transformation")
        transform_result = transform_data()
        logger.info(f"Transformation result: {transform_result}")

        # Step 3: Load data
        logger.info("Running data loading")
        load_result = load_data()
        logger.info(f"Loading result: {load_result}")

        logger.info("ETL pipeline completed successfully.")
        return {
            'extract_records': extract_result[0]['total_records'] if not use_csv else import_result[0]['total_records'],
            'transform_records': transform_result[0]['total_records'],
            'load_records': load_result
        }

    except Exception as e:
        logger.error(f"ETL pipeline failed: {e}")
        raise

def main():
    """Main function to run the ETL pipeline."""
    try:
        result = run_etl()
        logger.info(f"ETL pipeline completed. Extracted: {result['extract_records']}, Transformed: {result['transform_records']}, Loaded: {result['load_records']}")
        return result
    except Exception as e:
        logger.error(f"ETL pipeline failed: {e}")
        raise

if __name__ == "__main__":
    main()