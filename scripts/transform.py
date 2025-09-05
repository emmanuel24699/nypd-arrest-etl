import pandas as pd
import json
import logging
from datetime import datetime
import os

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

def transform_data():
    """Transform raw NYPD arrest data into a clean format."""
    try:
        # Read raw data
        logger.info(f"Reading raw data from {INPUT_PATH}")
        if not os.path.exists(INPUT_PATH):
            logger.error(f"Input file {INPUT_PATH} does not exist")
            raise FileNotFoundError(f"{INPUT_PATH} not found")
        
        with open(INPUT_PATH, 'r') as f:
            raw_data = json.load(f)
        
        logger.info(f"Loaded {len(raw_data)} raw records")
        df = pd.DataFrame(raw_data)

        # Log initial data shape and columns
        logger.info(f"Initial DataFrame shape: {df.shape}")
        logger.info(f"Columns: {list(df.columns)}")

        # Drop nested lon_lat column if present
        if 'lon_lat' in df.columns:
            df = df.drop(columns=['lon_lat'])
            logger.info("Dropped lon_lat column")

        # 1. Check for missing critical fields before dropping
        missing_key = df['arrest_key'].isna() | (df['arrest_key'] == '')
        missing_date = df['arrest_date'].isna() | (df['arrest_date'] == '')
        missing_rows = df[missing_key | missing_date]
        if not missing_rows.empty:
            logger.warning(f"Found {len(missing_rows)} rows with missing arrest_key or arrest_date")
            for idx, row in missing_rows.head(5).iterrows():  # Log up to 5 examples
                logger.warning(f"Missing data in row {idx}: arrest_key={row.get('arrest_key', 'N/A')}, arrest_date={row.get('arrest_date', 'N/A')}")

        # Drop rows with missing or empty critical fields
        initial_rows = df.shape[0]
        df = df[~(missing_key | missing_date)]
        logger.info(f"Dropped {initial_rows - df.shape[0]} rows with missing or empty arrest_key or arrest_date")

        # 2. Convert data types
        try:
            df['arrest_date'] = pd.to_datetime(df['arrest_date'], format='ISO8601', errors='coerce')
            df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
            df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
            df['arrest_precinct'] = pd.to_numeric(df['arrest_precinct'], errors='coerce')
            logger.info("Converted data types for arrest_date, latitude, longitude, arrest_precinct")
        except Exception as e:
            logger.error(f"Error converting data types: {e}")
            raise

        # 3. Handle missing values
        # Fill nullable fields with defaults
        df['pd_cd'] = df['pd_cd'].fillna('UNKNOWN')
        df['pd_desc'] = df['pd_desc'].fillna('UNKNOWN')
        df['ky_cd'] = df['ky_cd'].fillna('UNKNOWN')
        df['ofns_desc'] = df['ofns_desc'].fillna('UNKNOWN')
        df['law_code'] = df['law_code'].fillna('UNKNOWN')
        df['law_cat_cd'] = df['law_cat_cd'].fillna('U')
        df['arrest_boro'] = df['arrest_boro'].fillna('U')
        df['arrest_precinct'] = df['arrest_precinct'].fillna(-1)
        df['age_group'] = df['age_group'].fillna('UNKNOWN')
        df['perp_sex'] = df['perp_sex'].fillna('U')
        df['perp_race'] = df['perp_race'].fillna('UNKNOWN')
        df['latitude'] = df['latitude'].fillna(0.0)
        df['longitude'] = df['longitude'].fillna(0.0)
        df['x_coord_cd'] = df['x_coord_cd'].fillna('UNKNOWN')
        df['y_coord_cd'] = df['y_coord_cd'].fillna('UNKNOWN')
        logger.info("Filled missing values with defaults")

        # 4. Normalize data
        boro_map = {
            'K': 'Brooklyn',
            'M': 'Manhattan',
            'B': 'Bronx',
            'Q': 'Queens',
            'S': 'Staten Island',
            'U': 'Unknown'
        }
        df['arrest_boro'] = df['arrest_boro'].map(boro_map).fillna('Unknown')
        for col in ['pd_desc', 'ofns_desc', 'law_code', 'age_group', 'perp_race']:
            df[col] = df[col].str.upper()
        df['law_cat_cd'] = df['law_cat_cd'].str.upper()
        df['perp_sex'] = df['perp_sex'].str.upper()
        logger.info("Normalized borough codes and uppercased categorical fields")

        # 5. Handle duplicates
        if df['arrest_key'].duplicated().any():
            logger.warning("Found duplicate arrest_key values; removing duplicates")
            df = df.drop_duplicates(subset='arrest_key', keep='first')

        # 6. Log final data shape
        logger.info(f"Final DataFrame shape: {df.shape}")

        # 7. Save transformed data
        transformed_data = df.to_dict('records')
        try:
            os.makedirs('data', exist_ok=True)
            with open(OUTPUT_PATH, 'w') as f:
                json.dump(transformed_data, f, indent=2, default=str)
            logger.info(f"Saved {len(transformed_data)} transformed records to {OUTPUT_PATH}")
        except Exception as e:
            logger.error(f"Failed to save transformed data: {e}")
            raise

        return transformed_data

    except Exception as e:
        logger.error(f"Transformation failed: {e}")
        raise

def main():
    """Main function to run the transformation process."""
    try:
        transformed_data = transform_data()
        logger.info(f"Transformation complete. Total records: {len(transformed_data)}")
        return transformed_data
    except Exception as e:
        logger.error(f"Transformation failed: {e}")
        raise

if __name__ == "__main__":
    main()