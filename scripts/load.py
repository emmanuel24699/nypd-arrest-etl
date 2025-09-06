import psycopg2
import pandas as pd
import logging
from dotenv import load_dotenv
import os
from io import StringIO
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

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
CHUNK_SIZE = 100000
TEMP_TABLE = "nypd_arrests_temp"

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

def check_connection(conn):
    """Check if connection is open; reconnect if closed."""
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.close()
        return conn
    except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
        logger.warning(f"Connection closed: {e}. Reconnecting...")
        conn.close()
        return get_db_connection()

def create_temp_table(conn):
    """Create a regular table for bulk loading, dropping it if it exists."""
    create_query = f"""
    DROP TABLE IF EXISTS {TEMP_TABLE};
    CREATE TABLE {TEMP_TABLE} (
        arrest_key VARCHAR,
        arrest_date DATE,
        pd_cd VARCHAR,
        pd_desc VARCHAR,
        ky_cd VARCHAR,
        ofns_desc VARCHAR,
        law_code VARCHAR,
        law_cat_cd VARCHAR,
        arrest_boro VARCHAR,
        arrest_precinct INTEGER,
        jurisdiction_code VARCHAR,
        age_group VARCHAR,
        perp_sex VARCHAR,
        perp_race VARCHAR,
        x_coord_cd VARCHAR,
        y_coord_cd VARCHAR,
        latitude FLOAT,
        longitude FLOAT
    );
    """
    try:
        cur = conn.cursor()
        cur.execute(create_query)
        conn.commit()
        logger.info(f"Table {TEMP_TABLE} created.")
        cur.close()
    except Exception as e:
        logger.error(f"Failed to create table: {e}")
        raise

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type((psycopg2.OperationalError, psycopg2.InterfaceError))
)
def copy_chunk_to_table(conn, buffer, columns):
    """Copy chunk to table with retry logic."""
    try:
        cur = conn.cursor()
        cur.copy_expert(
            f"COPY {TEMP_TABLE} ({','.join(columns)}) FROM STDIN WITH (FORMAT CSV, DELIMITER '\t', NULL '')",
            buffer
        )
        conn.commit()
        logger.info(f"Copied {buffer.getvalue().count('\n')} records to table")
        cur.close()
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to copy chunk to table: {e}")
        raise

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type((psycopg2.OperationalError, psycopg2.InterfaceError))
)
def merge_into_main_table(conn):
    """Merge data from temporary table to nypd_arrests with ON CONFLICT and normalization."""
    merge_query = f"""
    INSERT INTO nypd_arrests (
        arrest_key, arrest_date, pd_cd, pd_desc, ky_cd, ofns_desc, law_code,
        law_cat_cd, arrest_boro, arrest_precinct, jurisdiction_code, age_group,
        perp_sex, perp_race, x_coord_cd, y_coord_cd, latitude, longitude
    )
    SELECT
        arrest_key,
        arrest_date,
        pd_cd,
        pd_desc,
        ky_cd,
        ofns_desc,
        law_code,
        CASE
            WHEN UPPER(law_cat_cd) IN ('F', 'M', 'V', 'I') THEN UPPER(law_cat_cd)
            ELSE 'U'
        END AS law_cat_cd,
        arrest_boro,
        arrest_precinct,
        jurisdiction_code,
        age_group,
        CASE
            WHEN UPPER(perp_sex) IN ('M', 'F') THEN UPPER(perp_sex)
            ELSE 'U'
        END AS perp_sex,
        perp_race,
        x_coord_cd,
        y_coord_cd,
        latitude,
        longitude
    FROM {TEMP_TABLE}
    ON CONFLICT (arrest_key) DO NOTHING;
    """
    try:
        cur = conn.cursor()
        cur.execute(merge_query)
        inserted = cur.rowcount
        conn.commit()
        logger.info(f"Merged {inserted} records into nypd_arrests.")
        cur.close()
        return inserted
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to merge data: {e}")
        raise

def chunk_to_stringio(chunk):
    """Convert a DataFrame chunk to a tab-delimited StringIO buffer."""
    buffer = StringIO()
    chunk = chunk.fillna('')
    chunk.to_csv(buffer, sep='\t', index=False, header=False, na_rep='')
    buffer.seek(0)
    return buffer

def load_data():
    """Load transformed data into nypd_arrests table using COPY with StringIO."""
    try:
        logger.info(f"Reading transformed data from {INPUT_PATH}")
        if not os.path.exists(INPUT_PATH):
            logger.error(f"Input file {INPUT_PATH} does not exist")
            raise FileNotFoundError(f"{INPUT_PATH} not found")

        conn = get_db_connection()
        total_inserted = 0

        create_temp_table(conn)

        columns = [
            'arrest_key', 'arrest_date', 'pd_cd', 'pd_desc', 'ky_cd', 'ofns_desc',
            'law_code', 'law_cat_cd', 'arrest_boro', 'arrest_precinct',
            'jurisdiction_code', 'age_group', 'perp_sex', 'perp_race',
            'x_coord_cd', 'y_coord_cd', 'latitude', 'longitude'
        ]

        for chunk in pd.read_json(INPUT_PATH, chunksize=CHUNK_SIZE, lines=True):
            logger.info(f"Processing chunk: {len(chunk)} records")

            chunk = chunk.reindex(columns=columns, fill_value='')

            buffer = chunk_to_stringio(chunk)

            # Check and reconnect if necessary
            conn = check_connection(conn)

            try:
                copy_chunk_to_table(conn, buffer, columns)
            finally:
                buffer.close()

            conn = check_connection(conn)
            inserted = merge_into_main_table(conn)
            total_inserted += inserted

            try:
                cur = conn.cursor()
                cur.execute(f"DROP TABLE IF EXISTS {TEMP_TABLE};")
                conn.commit()
                logger.info(f"Dropped table {TEMP_TABLE}")
                cur.close()
            except Exception as e:
                logger.error(f"Failed to drop table: {e}")
                raise

            conn = check_connection(conn)
            create_temp_table(conn)

        conn.close()
        logger.info(f"Total records inserted: {total_inserted}")
        return total_inserted

    except Exception as e:
        logger.error(f"Loading failed: {e}")
        raise
    finally:
        if 'conn' in locals() and not conn.closed:
            conn.close()
            logger.info("Database connection closed.")

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