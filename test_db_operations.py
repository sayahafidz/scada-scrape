import pyodbc
import logging
import random
import string
from datetime import datetime

# Set up logging
logging.basicConfig(
    filename='db_operations.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Database connection parameters
connection_string = (
    "Driver={ODBC Driver 18 for SQL Server};" # untuk linux server
    # "Driver={SQL Server};" untuk windows
    "Server=38.47.80.152,1433;"
    "Database=IOT_MILL;"
    "UID=iot_mill_user_1;"
    "PWD=i09c332s;"
    "Encrypt=no;"
)

def random_string(length=10):
    """Generate a random string of fixed length."""
    letters = string.ascii_letters  # A-Z, a-z
    return ''.join(random.choice(letters) for _ in range(length))

def random_float(min_value=0, max_value=100):
    """Generate a random float between min_value and max_value."""
    return round(random.uniform(min_value, max_value), 2)

def test_database_operations():
    conn = None
    cursor = None
    try:
        # Connect to the database
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        logging.info("Connection successful!")

        # Test data for insertion
        timestamp = datetime.now()  # Current timestamp
        value = random_float()       # Generate a random float value
        data_from = random_string()  # Generate a random source string

        # Insert data
        insert_query = """
            INSERT INTO PLC_BOILER_1_FinalPV_OGTTX (Timestamp, Value, Data_From) 
            VALUES (?, ?, ?)
        """
        cursor.execute(insert_query, (timestamp, value, data_from))
        conn.commit()
        logging.info(f"Inserted: Timestamp={timestamp}, Value={value}, Data_From={data_from}")

        # Retrieve data to verify insertion
        select_query = """
            SELECT * FROM PLC_BOILER_1_FinalPV_OGTTX 
            WHERE Value = ?
        """
        cursor.execute(select_query, (value,))
        rows = cursor.fetchall()

        logging.info("Retrieved rows:")
        for row in rows:
            logging.info(row)

        # Delete data
        delete_query = """
            DELETE FROM PLC_BOILER_1_FinalPV_OGTTX 
            WHERE Timestamp = ? AND Value = ? AND Data_From = ?
        """
        cursor.execute(delete_query, (timestamp, value, data_from))
        conn.commit()
        logging.info(f"Deleted: Timestamp={timestamp}, Value={value}, Data_From={data_from}")

        # Final success message
        logging.info("All database operations completed successfully.")

    except Exception as e:
        logging.error("An error occurred: %s", e)
    finally:
        # Close the cursor and connection
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()
            logging.info("Database connection closed.")

if __name__ == "__main__":
    test_database_operations()
