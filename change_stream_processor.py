import json
from pymongo import MongoClient
import psycopg2

# Define MongoDB and PostgreSQL connection parameters
MONGO_URI = 'mongodb://localhost:27017/'
POSTGRES_DB = 'database_name'
POSTGRES_USER = 'postgres_user'
POSTGRES_PASSWORD = 'postgres_password'


def connect_to_mongodb(mongo_uri):
    """Connect to MongoDB and return the MongoDB client."""
    try:
        client = MongoClient(mongo_uri)
        return client
    except Exception as e:
        print(f"Error connecting to MongoDB: {str(e)}")
        return None


def insert_data_to_mongodb(count, last_price, current_price):
    try:
        client = MongoClient(MONGO_URI)
        collection = client.changestream.collection

        for i in range(count):
            data = {
                "last_price": last_price + i,
                "current_price": current_price + i,
                "price_in_percentage": (current_price * 0.10),
                "increment_value": i + 1
            }
            collection.insert_one(data)

        print(f"{count} records inserted into MongoDB successfully.")
    except Exception as e:
        print(f"Error inserting data into MongoDB: {str(e)}")


def fetch_data_from_mongodb(mongo_client):
    """Fetch data from MongoDB and return it as a list."""
    if mongo_client:
        try:
            collection = mongo_client.changestream.collection
            all_data = list(collection.find())
            return all_data
        except Exception as e:
            print(f"Error fetching data from MongoDB: {str(e)}")
    return []


def create_postgresql_table():
    """Create the PostgreSQL table if it does not exist."""
    try:
        conn = psycopg2.connect(dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD, host='localhost')
        cursor = conn.cursor()
        create_table_query = """
        CREATE TABLE IF NOT EXISTS stream (
            id SERIAL PRIMARY KEY,
            last_price FLOAT NOT NULL,
            current_price FLOAT NOT NULL,
            price_in_percentage VARCHAR(10) NOT NULL,
            result JSON NOT NULL
        );
        """
        cursor.execute(create_table_query)
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error creating PostgreSQL table: {str(e)}")


def process_data_and_insert_to_postgresql(all_data):
    """Process data from MongoDB and insert it into PostgreSQL."""
    conn = psycopg2.connect(dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD, host='localhost')
    cursor = conn.cursor()

    previous_last_price = 0
    for data in all_data:
        last_price = data.get("last_price")
        current_price = data.get("current_price")
        price_in_percentage = data.get("price_in_percentage")

        new_price = current_price - previous_last_price
        actual_price = price_in_percentage / 100
        future_price = current_price * actual_price

        result = {
            'new_price': new_price,
            'actual_price': actual_price,
            'future_price': future_price
        }

        insert_query = "INSERT INTO stream (last_price, current_price, price_in_percentage, result) VALUES (%s, %s, %s, %s)"
        values = (previous_last_price, current_price, price_in_percentage, json.dumps(result))
        cursor.execute(insert_query, values)
        conn.commit()
        previous_last_price = last_price

    cursor.close()
    conn.close()


def main():
    # Step 1: Connect to MongoDB
    mongo_client = connect_to_mongodb(MONGO_URI)

    # Step 2: Insert data into MongoDB
    insert_data_to_mongodb(10, 100, 100)

    # Step 3: Fetch data from MongoDB
    all_data = fetch_data_from_mongodb(mongo_client)

    # Step 4: Create PostgreSQL table if not exists
    create_postgresql_table()

    # Step 5: Process data and insert into PostgreSQL
    process_data_and_insert_to_postgresql(all_data)


if __name__ == "__main__":
    main()
