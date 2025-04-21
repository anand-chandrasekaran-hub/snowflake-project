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

    # Optional: Set context (uncomment if needed)
  #  cursor.execute("USE DATABASE NECDEV_BW")
    #cursor.execute("USE SCHEMA BW_ADSO")

for table in metadata["tables"]:
    if table["name"] == "DIM_ADDRESSINFO":
        print(f"Creating table {table['name']}...")
        
        # Generate the DDL using Jinja2 template
        ddl = template.render(table=table)
        cursor.execute(ddl)
        print(f"Table {table['name']} created successfully.")

cursor.close()
conn.close()
