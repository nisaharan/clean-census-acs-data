# -----------------------------------------------------------------------------
#  Importing Required Libraries
# -----------------------------------------------------------------------------
# The following libraries are imported to provide essential functionality for the script:
#   - os, json: For file and data handling.
#   - sqlite3: For lightweight local database operations (tracking API usage).
#   - requests: For making HTTP requests to the Census API.
#   - logging: For logging progress and errors throughout the script.
#   - time: For managing delays and timestamps.
#   - pandas: For data manipulation and analysis.
#   - datetime: For working with dates and times, especially for logging and data records.
#   - dotenv: For loading environment variables from a .env file (API keys, credentials).
#   - sqlalchemy: For connecting to and executing SQL on Redshift databases.
#   - tenacity: For implementing retry logic with exponential backoff on API calls.
#   - boto3: For interacting with AWS S3 (uploading and managing files).
import os
import json
import sqlite3
import requests
import logging
import time
import pandas as pd
from datetime import datetime, date, timezone
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from tenacity import retry, stop_after_attempt, wait_random_exponential, retry_if_exception_type, RetryError
import boto3

# -----------------------------------------------------------------------------
#  Configuration & Logging
# -----------------------------------------------------------------------------
load_dotenv()
API_KEY = os.getenv("CENSUS_API_KEY")
DB_PATH = "census_api_usage.db"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

# -----------------------------------------------------------------------------
#  0. Load Excel-based Column Mapping
# -----------------------------------------------------------------------------
try:
    mapping_df = pd.read_excel("Census Data Fields for API.xlsx")
    mapping_df['API pull code'] = mapping_df['API pull code'].str.upper().str.strip()
    column_mapping = dict(
        zip(
            mapping_df['API pull code'],
            mapping_df['Relevant Field in Tableau Extract']
        )
    )
    logging.info(f"Loaded {len(column_mapping)} column mappings from Excel")
except Exception as e:
    logging.error(f"Failed to load Excel mapping file: {e}")
    raise

# -----------------------------------------------------------------------------
#  1. Database Utility: Initialize usage table
# -----------------------------------------------------------------------------
def init_db(db_path: str = DB_PATH):
    """
    Initializes the local SQLite database for tracking API usage.
    Creates a 'usage' table if it does not already exist. This table stores
    the timestamp, endpoint, parameters, and headers for each API call made.
    """
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS usage (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT    NOT NULL,
                endpoint  TEXT    NOT NULL,
                params    TEXT,
                headers   TEXT    NOT NULL
            )
            """
        )
        conn.commit()

# -----------------------------------------------------------------------------
#  2. Log API usage to SQLite
# -----------------------------------------------------------------------------
def log_usage(endpoint: str, params: dict, headers: requests.structures.CaseInsensitiveDict,
              db_path: str = DB_PATH):
    """
    Logs each API call to the local SQLite database.
    Stores the endpoint, parameters, and relevant rate limit headers for auditing and debugging.
    """
    rl_headers = {k: v for k, v in headers.items() if k.lower().startswith("x-ratelimit")}
    record = (
        datetime.now(timezone.utc).isoformat(),
        endpoint,
        json.dumps(params, ensure_ascii=False),
        json.dumps(rl_headers, ensure_ascii=False),
    )
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO usage (timestamp, endpoint, params, headers) VALUES (?, ?, ?, ?)",
            record
        )
        conn.commit()

# -----------------------------------------------------------------------------
#  3. Wrapped GET that logs usage
# -----------------------------------------------------------------------------
def census_get(url: str, params: dict) -> requests.Response:
    """
    Makes a GET request to the Census API, automatically appending the API key.
    Logs the API usage and raises an error if the request fails.
    Returns the response object for further processing.
    """
    p = params.copy()
    p["key"] = API_KEY
    resp = requests.get(url, params=p, timeout=60)
    log_usage(url, p, resp.headers)
    resp.raise_for_status()
    return resp

# -----------------------------------------------------------------------------
#  4. Fetch available variables via metadata
# -----------------------------------------------------------------------------
def filter_available_variables(year: int, dataset: str, variables: list) -> list:
    """
    Checks which variables from the provided list are available in the specified Census dataset and year.
    Returns a filtered list of valid variable names.
    """
    meta_url = f'https://api.census.gov/data/{year}/{dataset}/variables.json'
    resp = census_get(meta_url, {})
    available = set(resp.json().get('variables', {}).keys())
    return [v for v in variables if v in available]

# -----------------------------------------------------------------------------
#  5. Fetch Census data with retry/backoff
# -----------------------------------------------------------------------------
@retry(
    stop=stop_after_attempt(5),
    wait=wait_random_exponential(multiplier=1, max=60),
    retry=retry_if_exception_type(requests.exceptions.HTTPError)
)
def get_census_data(
    year: int,
    dataset: str,
    variables: list,
    state: str,
    all_vars: list
) -> pd.DataFrame:
    """
    Fetches Census data for the specified year, dataset, variables, and state(s).
    Retries on HTTP errors with exponential backoff. Returns a DataFrame with all requested columns,
    filling missing columns with None if necessary.
    """
    vars_for_request = ['NAME'] + variables
    vars_for_request = list(dict.fromkeys(vars_for_request))

    response = census_get(
        f'https://api.census.gov/data/{year}/{dataset}',
        {
            'get': ','.join(vars_for_request),
            'for': 'tract:*',
            'in': f'state:{state}'
        }
    )

    data = response.json()
    df = pd.DataFrame(data[1:], columns=data[0])
    df['state_fips'] = state

    for missing in set(all_vars) - set(df.columns):
        df[missing] = None

    df = df[[c for c in all_vars if c in df.columns] +
            [c for c in df.columns if c not in all_vars]]
    return df

# -----------------------------------------------------------------------------
#  6. Redshift & S3 config
# -----------------------------------------------------------------------------
REDSHIFT_CONFIG = {
    'host': os.getenv('REDSHIFT_HOST'),
    'port': os.getenv('REDSHIFT_PORT'),
    'database': os.getenv('REDSHIFT_DATABASE'),
    'user': os.getenv('REDSHIFT_USER'),
    'password': os.getenv('REDSHIFT_PASSWORD'),
    'schema': os.getenv('REDSHIFT_SCHEMA', 'census_tract_2023'),
    'timeout': 30
}

connection_url = URL.create(
    drivername='redshift+redshift_connector',
    username=REDSHIFT_CONFIG['user'],
    password=REDSHIFT_CONFIG['password'],
    host=REDSHIFT_CONFIG['host'],
    port=REDSHIFT_CONFIG['port'],
    database=REDSHIFT_CONFIG['database']
)

engine = create_engine(
    connection_url,
    connect_args={'timeout': REDSHIFT_CONFIG['timeout'], 'sslmode': 'prefer'},
    execution_options={'autocommit': True, 'isolation_level': 'AUTOCOMMIT'}
)

S3_BUCKET = 'nigen'
S3_PREFIX = 'Demographics'
REDSHIFT_IAM_ROLE_ARN = os.getenv('REDSHIFT_IAM_ROLE_ARN')
AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID', '')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY', '')

# -----------------------------------------------------------------------------
#  7. Redshift table & S3 utilities
# -----------------------------------------------------------------------------
# This section contains utility functions for interacting with AWS Redshift and S3.
# Functions here help automate table creation, data upload, and other Redshift/S3 tasks.
# You can add more functions below for:
#   - Loading data from S3 to Redshift
#   - Deleting S3 objects
#   - Querying Redshift tables
#   - Managing Redshift schemas
#   - Any other Redshift/S3 related utilities

def create_redshift_table(table_name: str, columns: list) -> None:
    """
    Creates a new table in the specified Redshift schema with the given columns.
    Drops the table if it already exists to ensure a clean load. All columns are created as VARCHAR(255).
    """
    schema = REDSHIFT_CONFIG['schema']
    column_defs = [f'"{col}" VARCHAR(255)' for col in columns]
    ddl = f"""
    CREATE TABLE IF NOT EXISTS "{schema}"."{table_name}" (
        {', '.join(column_defs)}
    )
    DISTSTYLE EVEN
    SORTKEY (state_fips, county, tract);
    """
    with engine.connect() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}";'))
        conn.execute(text(f'DROP TABLE IF EXISTS "{schema}"."{table_name}";'))
        conn.execute(text(ddl))
    logging.info(f"Created table {schema}.{table_name}")


def upload_csv_to_s3(table_name: str, df: pd.DataFrame) -> str:
    """
    Uploads a pandas DataFrame as a CSV file to an S3 bucket under a timestamped key.
    Returns the S3 URI of the uploaded file for use in Redshift COPY operations.
    """
    timestamp = int(time.time())
    local_csv = f"{table_name}_{timestamp}.csv"
    s3_key = f"{S3_PREFIX}/{REDSHIFT_CONFIG['schema']}/{table_name}/census_{timestamp}.csv"
    df.to_csv(local_csv, index=False)

    s3 = boto3.client(
        's3',
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    with open(local_csv, 'rb') as f:
        s3.upload_fileobj(f, S3_BUCKET, s3_key)
    os.remove(local_csv)
    return f"s3://{S3_BUCKET}/{s3_key}"


def copy_from_s3_to_redshift(table_name: str, s3_uri: str) -> None:
    """
    Loads data from a CSV file in S3 into a Redshift table using the COPY command.
    Assumes the table already exists and the IAM role has appropriate permissions.
    """
    schema = REDSHIFT_CONFIG['schema']
    copy_sql = f"""
    COPY "{schema}"."{table_name}" FROM '{s3_uri}'
    IAM_ROLE '{REDSHIFT_IAM_ROLE_ARN}'
    CSV IGNOREHEADER 1 EMPTYASNULL BLANKSASNULL
    DELIMITER ',' REGION '{AWS_REGION}';
    """
    with engine.connect() as conn:
        conn.execute(text(copy_sql))
    logging.info(f"Loaded data into {schema}.{table_name}")

# -----------------------------------------------------------------------------
#  8. Pipeline datasets & main
# -----------------------------------------------------------------------------
datasets = {
    'dp02_2023': {
        'variables': ['NAME', 'DP02_0060E', 'DP02_0061E', 'DP02_0062E', 'DP02_0063E', 'DP02_0064E', 'DP02_0068E'],
        'dataset': 'acs/acs5/profile'
    },
    'dp03_2023': {
        'variables': ['NAME', 'DP03_0062E', 'DP03_0052E', 'DP03_0053E', 'DP03_0054E', 'DP03_0055E',
                      'DP03_0056E', 'DP03_0057E', 'DP03_0058E', 'DP03_0059E', 'DP03_0060E', 'DP03_0061E',
                      'DP03_0097PE', 'DP03_0009PE'],
        'dataset': 'acs/acs5/profile'
    },
    'dp05_2023': {
        'variables': ['NAME','DP05_0076E', 'DP05_0082E', 'DP05_0083E', 'DP05_0084E','DP05_0085E' ,'DP05_0086E' ,'DP05_0087E' ,'DP05_0088E'],
        'dataset': 'acs/acs5/profile'
    },
    's0101_2023': {
        'variables': ['NAME', 'S0101_C01_001E', 'S0101_C01_002E', 'S0101_C01_003E', 'S0101_C01_004E',
                      'S0101_C01_005E', 'S0101_C01_006E', 'S0101_C01_007E', 'S0101_C01_008E', 'S0101_C01_009E',
                      'S0101_C01_010E', 'S0101_C01_011E', 'S0101_C01_012E', 'S0101_C01_013E', 'S0101_C01_014E',
                      'S0101_C01_015E', 'S0101_C01_016E', 'S0101_C01_017E', 'S0101_C01_018E', 'S0101_C01_019E',
                      'S0101_C01_020E', 'S0101_C01_021E', 'S0101_C01_022E', 'S0101_C01_023E', 'S0101_C01_024E',
                      'S0101_C01_025E', 'S0101_C01_026E', 'S0101_C01_027E', 'S0101_C01_028E', 'S0101_C01_029E',
                      'S0101_C01_030E', 'S0101_C01_031E', 'S0101_C01_032E', 'S0101_C03_001E',
                      'S0101_C05_001E', 'S0101_C05_024E'],
        'dataset': 'acs/acs5/subject'
    }
}


def main():
    """
    Main pipeline function that orchestrates the entire ETL process:
      - Initializes the usage tracking database
      - Iterates through each dataset configuration
      - Fetches Census data in state chunks, with error handling and retries
      - Normalizes and maps columns
      - Loads the data into Redshift via S3
    """
    init_db()
    # All 50 states + DC
    states = [f"{i:02}" for i in range(1,57) if i not in [3,7,14,43,52]]

    def chunk_list(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i+n]

    for table_name, config in datasets.items():
        expected = config['variables']
        valid = filter_available_variables(2023, config['dataset'], expected)
        if not valid:
            logging.warning(f"No available vars for {table_name}, skipping.")
            continue

        all_data = []
        # Use 3-state chunks by default
        for chunk in chunk_list(states, 3):
            state_str = ",".join(chunk)
            time.sleep(10)

            try:
                df = get_census_data(2023, config['dataset'], valid, state_str, expected)
                if df.empty:
                    continue

                # normalize and map columns
                df.columns = [c.replace('-', '_').upper().strip() for c in df.columns]
                orig = df.columns.tolist()
                df.rename(columns=lambda c: column_mapping.get(c, c), inplace=True)

                unmapped = set(orig) - set(column_mapping.keys())
                unmapped -= {'TRACT','COUNTY','STATE','STATE_FIPS','NAME'}
                if unmapped:
                    logging.warning(f"Unmapped cols in {state_str}: {unmapped}")

                all_data.append(df)
                logging.info(f"Collected chunk {state_str} for table {table_name}")

            except RetryError as re:
                http_err = re.last_attempt.exception()
                resp = getattr(http_err, 'response', None)
                code = resp.status_code if resp else 'N/A'
                text = resp.text[:200] if resp else str(http_err)
                logging.error(f"Chunk {state_str} skipped: HTTP {code} – {text}")
                # Fallback: try each state individually
                # If a chunk of states fails to fetch due to an HTTP error, log the error with details.
                # As a fallback, attempt to fetch data for each state in the failed chunk individually.
                for s in chunk:
                    time.sleep(10)  # Pause to avoid hitting API rate limits.
                    try:
                        # Try to fetch census data for a single state.
                        df_single = get_census_data(2023, config['dataset'], valid, s, expected)
                        if df_single.empty:
                            # If no data is returned, skip to the next state.
                            continue
                        # Standardize column names by replacing dashes with underscores, uppercasing, and stripping whitespace.
                        df_single.columns = [c.replace('-', '_').upper().strip() for c in df_single.columns]
                        # Rename columns using the mapping from the Excel file, if available.
                        df_single.rename(columns=lambda c: column_mapping.get(c, c), inplace=True)
                        # Add the DataFrame for this state to the list of all data.
                        all_data.append(df_single)
                        logging.info(f"Recovered state {s} after chunk failure")
                    except Exception as e_single:
                        # If fetching data for a single state fails, log the error and skip that state.
                        logging.error(f"State {s} permanently skipped: {e_single}")

            except Exception as e:
                # Catch any other exceptions during chunk processing and log the error.
                logging.error(f"Skipping chunk {state_str}: {e}")

        if all_data:
            # If any data was collected for the current table, concatenate all DataFrames into one.
            combined = pd.concat(all_data, ignore_index=True)
            try:
                # Create the corresponding Redshift table with the appropriate columns.
                create_redshift_table(table_name, combined.columns.tolist())
                # Upload the combined DataFrame as a CSV to S3 and get the S3 URI.
                s3_uri = upload_csv_to_s3(table_name, combined)
                # Load the CSV data from S3 into the Redshift table.
                copy_from_s3_to_redshift(table_name, s3_uri)
            except Exception as e:
                # Log any errors that occur during the Redshift/S3 loading process.
                logging.error(f"Failed loading table {table_name}: {e}")

if __name__ == '__main__':
    # Entry point for the script. Runs the main pipeline and logs completion.
    main()
    logging.info("Pipeline completed")
