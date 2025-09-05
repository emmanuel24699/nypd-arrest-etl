import psycopg2
from dotenv import load_dotenv
import os
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data/setup_db.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

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

def create_table():
    """Create the nypd_arrests table if it doesn't exist."""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS nypd_arrests (
        arrest_key VARCHAR PRIMARY KEY,
        arrest_date DATE NOT NULL,
        pd_cd VARCHAR,
        pd_desc VARCHAR,
        ky_cd VARCHAR,
        ofns_desc VARCHAR,
        law_code VARCHAR,
        law_cat_cd CHAR(1),
        arrest_boro VARCHAR,
        arrest_precinct INTEGER,
        jurisdiction_code VARCHAR,
        age_group VARCHAR,
        perp_sex CHAR(1),
        perp_race VARCHAR,
        x_coord_cd VARCHAR,
        y_coord_cd VARCHAR,
        latitude FLOAT,
        longitude FLOAT
    );
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(create_table_query)
        conn.commit()
        logger.info("Table nypd_arrests created or already exists.")
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"Failed to create table: {e}")
        raise

def main():
    """Main function to set up the database."""
    try:
        create_table()
        logger.info("Database setup complete.")
    except Exception as e:
        logger.error(f"Database setup failed: {e}")
        raise

if __name__ == "__main__":
    main()