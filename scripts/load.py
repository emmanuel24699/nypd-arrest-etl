import psycopg2
import json
import logging
from dotenv import load_dotenv
import os
from psycopg2.extras import execute_values

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data/load.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Constants
INPUT_PATH = "data/transformed_data.json"
BATCH_SIZE = 1000

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

def load_data():
    """Load transformed data into nypd_arrests table."""
    try:
        # Read transformed data
        logger.info(f"Reading transformed data from {INPUT_PATH}")
        if not os.path.exists(INPUT_PATH):
            logger.error(f"Input file {INPUT_PATH} does not exist")
            raise FileNotFoundError(f"{INPUT_PATH} not found")
        
        with open(INPUT_PATH, 'r') as f:
            data = json.load(f)
        
        logger.info(f"Loaded {len(data)} transformed records")

        # Connect to database
        conn = get_db_connection()
        cur = conn.cursor()

        # Prepare insert query
        insert_query = """
        INSERT INTO nypd_arrests (
            arrest_key, arrest_date, pd_cd, pd_desc, ky_cd, ofns_desc, law_code,
            law_cat_cd, arrest_boro, arrest_precinct, jurisdiction_code, age_group,
            perp_sex, perp_race, x_coord_cd, y_coord_cd, latitude, longitude
        ) VALUES %s
        ON CONFLICT (arrest_key) DO NOTHING;
        """
        # Convert data to tuples for batch insertion
        records = [
            (
                record['arrest_key'],
                record['arrest_date'],
                record.get('pd_cd', 'UNKNOWN'),
                record.get('pd_desc', 'UNKNOWN'),
                record.get('ky_cd', 'UNKNOWN'),
                record.get('ofns_desc', 'UNKNOWN'),
                record.get('law_code', 'UNKNOWN'),
                record.get('law_cat_cd', 'U'),
                record.get('arrest_boro', 'Unknown'),
                record.get('arrest_precinct', -1),
                record.get('jurisdiction_code', 'UNKNOWN'),
                record.get('age_group', 'UNKNOWN'),
                record.get('perp_sex', 'U'),
                record.get('perp_race', 'UNKNOWN'),
                record.get('x_coord_cd', 'UNKNOWN'),
                record.get('y_coord_cd', 'UNKNOWN'),
                record.get('latitude', 0.0),
                record.get('longitude', 0.0)
            )
            for record in data
        ]

        # Insert in batches
        total_inserted = 0
        for i in range(0, len(records), BATCH_SIZE):
            batch = records[i:i + BATCH_SIZE]
            try:
                execute_values(cur, insert_query, batch)
                conn.commit()
                total_inserted += len(batch)
                logger.info(f"Inserted batch {i // BATCH_SIZE + 1}: {len(batch)} records")
            except Exception as e:
                conn.rollback()
                logger.error(f"Failed to insert batch {i // BATCH_SIZE + 1}: {e}")
                raise
        
        logger.info(f"Total records inserted: {total_inserted}")
        cur.close()
        conn.close()
        return total_inserted

    except Exception as e:
        logger.error(f"Loading failed: {e}")
        raise

def main():
    """Main function to run the loading process."""
    try:
        total_inserted = load_data()
        logger.info(f"Loading complete. Total records inserted: {total_inserted}")
        return total_inserted
    except Exception as e:
        logger.error(f"Loading failed: {e}")
        raise

if __name__ == "__main__":
    main()