# This script pulls Census data from the API, uploads it to S3, and loads it into Redshift.
# It uses a mapping file to rename columns and supports multiple datasets.
#
# Author: Nisaharan Genhatharan
# Date: 05-31-2025

import os
import requests
import pandas as pd
import boto3
import psycopg2
from dotenv import load_dotenv
from io import StringIO

# Load environment variables from .env file
load_dotenv()

# Retrieve credentials and configurations from environment variables
API_KEY = os.getenv("API_KEY")
S3_BUCKET = "nigen"
S3_PREFIX = "Demographics"
AWS_REGION = os.getenv("AWS_REGION")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
REDSHIFT_DATABASE = os.getenv("REDSHIFT_DATABASE")
REDSHIFT_SCHEMA = os.getenv("REDSHIFT_SCHEMA")
REDSHIFT_USER = os.getenv("REDSHIFT_USER")
REDSHIFT_PASSWORD = os.getenv("REDSHIFT_PASSWORD")
REDSHIFT_HOST = os.getenv("REDSHIFT_HOST")
REDSHIFT_PORT = os.getenv("REDSHIFT_PORT")
REDSHIFT_IAM_ROLE_ARN = os.getenv("REDSHIFT_IAM_ROLE_ARN")

# Initialize S3 client for uploading files
s3_client = boto3.client(
    "s3",
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

# Load Excel mapping file for column renaming
mapping_df = pd.read_excel("Census Data Fields for API.xlsx")
# Standardize the API pull code column for consistent mapping
mapping_df['API pull code'] = mapping_df['API pull code'].str.upper().str.strip()
# Create a dictionary for column mapping
column_mapping = dict(zip(mapping_df['API pull code'], mapping_df['Relevant Field in Tableau Extract']))

def get_redshift_connection():
    """
    Establishes and returns a connection to the Redshift database using credentials from environment variables.
    Returns:
        psycopg2 connection object
    """
    return psycopg2.connect(
        dbname=REDSHIFT_DATABASE,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD,
        host=REDSHIFT_HOST,
        port=REDSHIFT_PORT
    )

def create_redshift_table(table_name, columns):
    """
    Creates a table in Redshift with the specified columns if it does not already exist.
    Args:
        table_name (str): Name of the table to create.
        columns (list): List of column names for the table.
    """
    quoted_schema = f'"{REDSHIFT_SCHEMA}"'
    # Build the CREATE TABLE SQL statement dynamically
    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {quoted_schema}.{table_name} (
            {', '.join([f'"{col}" VARCHAR(255)' for col in columns])}
        );
    """
    conn = get_redshift_connection()
    cursor = conn.cursor()
    cursor.execute(create_table_sql)
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Created Redshift table {quoted_schema}.{table_name} with columns: {columns}")

def get_census_data(year, dataset, variables, geo_level='zip code tabulation area', zcta='*'):
    """
    Fetches data from the US Census API for a given year, dataset, and variables.
    Args:
        year (int): The year of the data.
        dataset (str): The dataset endpoint (e.g., 'acs/acs5/profile').
        variables (list): List of variable codes to pull.
        geo_level (str): The geographic level (default is 'zip code tabulation area').
        zcta (str): The ZCTA code or '*' for all.
    Returns:
        pd.DataFrame or None: DataFrame with the data or None if request fails.
    """
    base_url = f'https://api.census.gov/data/{year}/{dataset}'
    params = {
        'get': ','.join(variables),
        'for': f'{geo_level}:{zcta}',
        'key': API_KEY
    }
    response = requests.get(base_url, params=params)
    if response.status_code == 200:
        data = response.json()
        return pd.DataFrame(data[1:], columns=data[0])
    else:
        print(f"Error {response.status_code}: {response.text}")
        return None

def upload_to_s3(df, table_name, year):
    """
    Uploads a DataFrame as a CSV file to an S3 bucket.
    Args:
        df (pd.DataFrame): The data to upload.
        table_name (str): The name of the table (used in filename).
        year (int): The year (used in filename).
    Returns:
        str: The S3 path where the file was uploaded.
    """
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    file_name = f"{table_name}_{year}.csv"
    s3_path = f"{S3_PREFIX}/{REDSHIFT_SCHEMA}/{file_name}"
    s3_client.put_object(Bucket=S3_BUCKET, Key=s3_path, Body=csv_buffer.getvalue())
    print(f"Uploaded {s3_path} to S3")
    return s3_path

def load_to_redshift(s3_file_path, table_name, column_names):
    """
    Loads data from a CSV file in S3 into a Redshift table using the COPY command.
    Args:
        s3_file_path (str): The S3 path to the CSV file.
        table_name (str): The Redshift table name.
        column_names (list): List of column names for the table.
    """
    conn = get_redshift_connection()
    cursor = conn.cursor()
    quoted_schema = f'"{REDSHIFT_SCHEMA}"'
    quoted_columns = ', '.join([f'"{col}"' for col in column_names])
    sql_copy = f"""
        COPY {quoted_schema}.{table_name} ({quoted_columns})
        FROM 's3://{S3_BUCKET}/{s3_file_path}'
        IAM_ROLE '{REDSHIFT_IAM_ROLE_ARN}'
        FORMAT AS CSV
        IGNOREHEADER 1
        BLANKSASNULL
        EMPTYASNULL
        ACCEPTINVCHARS;
    """
    cursor.execute(sql_copy)
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Loaded {s3_file_path} into Redshift table {table_name}")

# Dataset variables: specify which variables to pull from which datasets
# Each key is a table name, and the value is a dict with variables and dataset endpoint
# These variables correspond to Census API codes

datasets = {
    'dp02_2017': {
        'variables': ['NAME', 'DP02_0060E', 'DP02_0061E', 'DP02_0062E', 'DP02_0063E', 'DP02_0064E', 'DP02_0068E'],
        'dataset': 'acs/acs5/profile'
    },
    'dp03_2017': {
        'variables': ['NAME', 'DP03_0062E', 'DP03_0052E', 'DP03_0053E', 'DP03_0054E', 'DP03_0055E',
                      'DP03_0056E', 'DP03_0057E', 'DP03_0058E', 'DP03_0059E', 'DP03_0060E', 'DP03_0061E',
                      'DP03_0097PE', 'DP03_0009PE'],
        'dataset': 'acs/acs5/profile'
    },
    'dp05_2017': {
        'variables': ['NAME','DP05_0076E', 'DP05_0082E', 'DP05_0083E', 'DP05_0084E','DP05_0085E' ,'DP05_0086E' ,'DP05_0087E' ,'DP05_0088E'],
        'dataset': 'acs/acs5/profile'
    },
    's0101_2017': {
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

year = 2017

# Main ETL loop: for each dataset, pull data, map columns, upload to S3, and load to Redshift
for table_name, value in datasets.items():
    original_columns = value["variables"].copy()
    data = get_census_data(year, value["dataset"], original_columns)
    if data is not None:
        # Option A: explicitly include the API's "zip code tabulation area" field
        for geo_col in ['state', 'zip code tabulation area']:
            if geo_col in data.columns and geo_col not in original_columns:
                original_columns.append(geo_col)

        # Map and rename columns using the mapping file
        mapped_columns = [column_mapping.get(col.upper(), col) for col in original_columns]
        rename_dict   = {col: column_mapping.get(col.upper(), col) for col in data.columns}
        data.rename(columns=rename_dict, inplace=True)

        # Create the Redshift table and load the data
        create_redshift_table(table_name, mapped_columns)
        s3_file_path = upload_to_s3(data, table_name, year)
        load_to_redshift(s3_file_path, table_name, mapped_columns)
