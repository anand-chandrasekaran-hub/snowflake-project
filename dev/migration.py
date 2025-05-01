1# migration.py
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

# Load the private key from GitHub Secrets (it will be passed via environment variables in GitHub Actions)
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

# Define the SQL query to create or replace the table
sql_query = """
CREATE OR REPLACE TABLE NECDEV.DEV_STAGING1.Test (
    order_detail_id INT AUTOINCREMENT,
    order_id INT,
    product_id INT,
    quantity INT,
    PRIMARY KEY (order_detail_id),
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);
"""

# Execute the query
try:
    cursor = conn.cursor()
    cursor.execute(sql_query)
    print('Table created or replaced successfully.')
finally:
    cursor.close()
    conn.close()
