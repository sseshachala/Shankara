import psycopg2
import os

# Configuration for the database connection
db_config = {
    'dbname': 'defaultdb',
    'user': 'doadmin',
    'password': '',
    'host': '',
    'port': 25060
}

# Function to execute SQL commands from a file
def execute_sql_from_file(file_path):
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    try:
        # Read SQL commands from the file
        with open(file_path, 'r') as sql_file:
            sql_commands = sql_file.read()

        # Execute each SQL command
        commands = sql_commands.split(';')
        for command in commands:
            command = command.strip()
            if command:
                cursor.execute(command)
                print(f"Executed: {command}")

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
    sql_file_path = '/Users/ctp1126/Project/Shankara/backend/database/create.sql'  # Replace with the path to your SQL file

    # Execute SQL commands from the file
    execute_sql_from_file(sql_file_path)

if __name__ == '__main__':
    main()
