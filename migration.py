# migration.py

import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import os
import logging

# Set up logging for Snowflake and Botocore
for logger_name in ['snowflake', 'botocore']:
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    ch = logging.FileHandler('python_connector.log')
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(logging.Formatter(
        '%(asctime)s - %(threadName)s %(filename)s:%(lineno)d - %(funcName)s() - %(levelname)s - %(message)s'))
    logger.addHandler(ch)
    logger.info('Logging initialized.')

# Load the private key from environment variables (GitHub Secrets)
private_key = serialization.load_pem_private_key(
    bytes(os.getenv('SNOWFLAKE_PRIVATE_KEY'), 'utf-8'),
    password=bytes(os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE'), 'utf-8'),
    backend=default_backend()
)

# Establish the Snowflake connection
conn = snowflake.connector.connect(
    user=os.getenv('SNOWFLAKE_USER'),
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    private_key=private_key
)

# Create a cursor to execute SQL queries
cursor = conn.cursor()

try:
    # Optional: Set context (uncomment if needed)
    cursor.execute("USE DATABASE NECDEV_BW")
    cursor.execute("USE SCHEMA BW_ADSO")

    # Define the SQL query
    sql_query = """
    CREATE OR REPLACE TABLE NECDEV_BW.BW_ADSO.test (
        order_detail_id INT AUTOINCREMENT,
        order_id INT,
        product_id INT,
        quantity INT
    );
    """

    # Execute the query
    cursor.execute(sql_query)
    print("Table created or replaced successfully.")

except snowflake.connector.errors.ProgrammingError as e:
    print(f"Error executing SQL: {e}")

finally:
    # Clean up
    cursor.close()
    conn.close()
