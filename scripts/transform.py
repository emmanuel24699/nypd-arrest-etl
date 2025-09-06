import pandas as pd
import logging
import os
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data/transform.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Constants
INPUT_PATH = "data/raw_data.json"
OUTPUT_PATH = "data/transformed_data.json"
BOROUGH_MAPPING = {
    'B': 'Bronx',
    'K': 'Brooklyn',
    'M': 'Manhattan',
    'Q': 'Queens',
    'S': 'Staten Island'
}
LAW_CAT_CD_MAPPING = {
    'F': 'F',  # Felony
    'M': 'M',  # Misdemeanor
    'V': 'V',  # Violation
    'I': 'I',  # Infraction (less common, but valid in some datasets)
    '': 'U',   # Empty string to Unknown
    'NONE': 'U',  # Invalid values to Unknown
    None: 'U'     # Missing values to Unknown
}
CHUNK_SIZE = 100000

def convert_timestamp(value):
    """Convert Unix timestamp (milliseconds) to YYYY-MM-DD string."""
    try:
        # Try converting as a numeric timestamp (milliseconds)
        timestamp = float(value) / 1000  # Convert milliseconds to seconds
        dt = pd.to_datetime(timestamp, unit='s', utc=True)
        return dt.strftime('%Y-%m-%d')
    except (ValueError, TypeError):
        return value  # Return original value if conversion fails

def transform_data():
    """Transform raw data and save as JSON Lines."""
    try:
        # Read raw data
        logger.info(f"Reading raw data from {INPUT_PATH}")
        if not os.path.exists(INPUT_PATH):
            logger.error(f"Input file {INPUT_PATH} does not exist")
            raise FileNotFoundError(f"{INPUT_PATH} not found")

        # Initialize output file
        os.makedirs('data', exist_ok=True)
        with open(OUTPUT_PATH, 'w') as f:
            f.write('')  # Clear output file

        total_records = 0
        # Process JSON Lines in chunks
        for chunk in pd.read_json(INPUT_PATH, chunksize=CHUNK_SIZE, lines=True):
            logger.info(f"Processing chunk: {len(chunk)} records")
            logger.info(f"Chunk columns: {list(chunk.columns)}")

            # Rename columns to match expected names (case-insensitive)
            column_mapping = {col.lower(): col for col in chunk.columns}
            expected_columns = ['arrest_key', 'arrest_date']
            for col in expected_columns:
                if col not in chunk.columns and col.upper() in chunk.columns:
                    chunk = chunk.rename(columns={col.upper(): col})
                elif col not in chunk.columns:
                    logger.warning(f"Missing column {col} in chunk; filling with empty strings")
                    chunk[col] = ''

            # Convert columns to string where .str operations are used
            str_columns = ['arrest_key', 'arrest_date', 'pd_cd', 'pd_desc', 'ky_cd', 
                          'ofns_desc', 'law_code', 'law_cat_cd', 'arrest_boro', 
                          'jurisdiction_code', 'age_group', 'perp_sex', 'perp_race', 
                          'x_coord_cd', 'y_coord_cd']
            for col in str_columns:
                if col in chunk.columns:
                    chunk[col] = chunk[col].astype(str).replace('nan', '')

            # Normalize law_cat_cd to single character
            if 'law_cat_cd' in chunk.columns:
                chunk['law_cat_cd'] = chunk['law_cat_cd'].apply(
                    lambda x: LAW_CAT_CD_MAPPING.get(x.upper() if isinstance(x, str) else x, 'U')
                )
                logger.info("Normalized law_cat_cd to single character")

            # Drop lon_lat column if it exists
            if 'lon_lat' in chunk.columns:
                chunk = chunk.drop(columns=['lon_lat'])
                logger.info("Dropped lon_lat column")

            # Drop rows with missing or empty arrest_key or arrest_date
            initial_rows = len(chunk)
            chunk = chunk.dropna(subset=['arrest_key', 'arrest_date'])
            chunk = chunk[chunk['arrest_key'].str.strip() != '']
            chunk = chunk[chunk['arrest_date'].str.strip() != '']
            logger.info(f"Dropped {initial_rows - len(chunk)} rows with missing or empty arrest_key or arrest_date")

            # Convert arrest_date to datetime, handling timestamps
            try:
                # First try standard datetime conversion
                chunk['arrest_date'] = pd.to_datetime(chunk['arrest_date'], errors='coerce')
                # For remaining NaT values, try converting as Unix timestamps
                mask = chunk['arrest_date'].isna()
                if mask.any():
                    chunk.loc[mask, 'arrest_date'] = chunk.loc[mask, 'arrest_date'].apply(convert_timestamp)
                    # Retry datetime conversion after timestamp handling
                    chunk['arrest_date'] = pd.to_datetime(chunk['arrest_date'], errors='coerce')
                # Format as YYYY-MM-DD for PostgreSQL
                chunk['arrest_date'] = chunk['arrest_date'].dt.strftime('%Y-%m-%d')
                logger.info("Converted and formatted arrest_date to YYYY-MM-DD")
                
                # Convert numeric columns
                chunk['latitude'] = pd.to_numeric(chunk['latitude'], errors='coerce')
                chunk['longitude'] = pd.to_numeric(chunk['longitude'], errors='coerce')
                chunk['arrest_precinct'] = pd.to_numeric(chunk['arrest_precinct'], errors='coerce', downcast='integer')
                logger.info("Converted data types for latitude, longitude, arrest_precinct")
            except Exception as e:
                logger.error(f"Data type conversion failed: {e}")
                raise

            # Fill missing values
            chunk['pd_cd'] = chunk['pd_cd'].fillna('UNKNOWN')
            chunk['pd_desc'] = chunk['pd_desc'].fillna('UNKNOWN')
            chunk['ky_cd'] = chunk['ky_cd'].fillna('UNKNOWN')
            chunk['ofns_desc'] = chunk['ofns_desc'].fillna('UNKNOWN')
            chunk['law_code'] = chunk['law_code'].fillna('UNKNOWN')
            chunk['law_cat_cd'] = chunk['law_cat_cd'].fillna('U')
            chunk['arrest_boro'] = chunk['arrest_boro'].fillna('Unknown')
            chunk['arrest_precinct'] = chunk['arrest_precinct'].fillna(-1)
            chunk['jurisdiction_code'] = chunk['jurisdiction_code'].fillna('UNKNOWN')
            chunk['age_group'] = chunk['age_group'].fillna('UNKNOWN')
            chunk['perp_sex'] = chunk['perp_sex'].fillna('U')
            chunk['perp_race'] = chunk['perp_race'].fillna('UNKNOWN')
            chunk['x_coord_cd'] = chunk['x_coord_cd'].fillna('UNKNOWN')
            chunk['y_coord_cd'] = chunk['y_coord_cd'].fillna('UNKNOWN')
            chunk['latitude'] = chunk['latitude'].fillna(0.0)
            chunk['longitude'] = chunk['longitude'].fillna(0.0)
            logger.info("Filled missing values with defaults")

            # Normalize borough codes
            chunk['arrest_boro'] = chunk['arrest_boro'].map(BOROUGH_MAPPING).fillna(chunk['arrest_boro'])
            logger.info("Normalized borough codes")

            # Uppercase categorical fields
            categorical_columns = ['pd_cd', 'pd_desc', 'ky_cd', 'ofns_desc', 'law_code', 
                                  'law_cat_cd', 'arrest_boro', 'jurisdiction_code', 
                                  'age_group', 'perp_sex', 'perp_race']
            for col in categorical_columns:
                if col in chunk.columns:
                    chunk[col] = chunk[col].str.upper()
            logger.info("Uppercased categorical fields")

            # Append to JSON Lines output
            try:
                with open(OUTPUT_PATH, 'a') as f:
                    chunk.to_json(f, orient='records', lines=True, index=False)
                total_records += len(chunk)
                logger.info(f"Appended {len(chunk)} transformed records to {OUTPUT_PATH}, total: {total_records}")
            except Exception as e:
                logger.error(f"Failed to save transformed chunk: {e}")
                raise

        logger.info(f"Transformation complete. Total records: {total_records}")
        return [{'total_records': total_records}]

    except Exception as e:
        logger.error(f"Transformation failed: {e}")
        raise

def main():
    """Main function to run the transformation process."""
    try:
        transformed_data = transform_data()
        logger.info(f"Transformation complete. Total records: {transformed_data[0]['total_records']}")
        return transformed_data
    except Exception as e:
        logger.error(f"Transformation failed: {e}")
        raise

if __name__ == "__main__":
    main()