import psycopg2
import os
import json

# Configuration for the database connection
db_config = {
    'dbname': 'defaultdb',
    'user': 'doadmin',
    'password': '',
    'host': '',
    'port': 25060
}

# Function to execute SQL commands from a file
def execute_sql_from_file():

     # Load environment variables
    with open('.env.json') as f:
        env = json.load(f)
    print(env)
    
    db_config= {
        'dbname': env.get('PG_DBNAME'),
        'user': env.get('PG_USER'),
        'password': env.get('PG_PASSWORD'),
        'host': env.get('PG_HOST'),
        'port': env.get('PG_PORT')
    }
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    print(conn)

    try:
       cursor.execute("select * from shankara_embeddings")
        # Commit the changes
       conn.commit()

    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()

# Main function
def main():
    # Path to the SQL file
    # sql_file_path = '/Users/ctp1126/Project/Shankara/backend/database/create.sql'  # Replace with the path to your SQL file

    # Execute SQL commands from the file
    execute_sql_from_file()

if __name__ == '__main__':
    main()
