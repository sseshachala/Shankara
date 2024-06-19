import os
import json
import logging
from llama_index.core import SimpleDirectoryReader
from llama_index.embeddings.openai import OpenAIEmbedding
from llama_index.core import Settings
import psycopg2
from psycopg2.extras import execute_values

# Configuration
storage_directory = '../data'

# Function to chunk text with overlap
def chunk_text(text, chunk_size=1000, chunk_overlap=200):
    chunks = []
    start = 0
    while start < len(text):
        end = start + chunk_size
        chunks.append(text[start:end])
        start = end - chunk_overlap
    return chunks

# Function to create embeddings using OpenAI API
def create_embeddings(text_chunks, model="text-embedding-3-large", batch_size=10):
    embed_model = OpenAIEmbedding(embed_batch_size=batch_size)
    Settings.embed_model = embed_model
    logging.info(f"Creating embeddings... for {model} with batch size {batch_size}")
    embed_model = OpenAIEmbedding(model=model)
    return [embed_model.get_text_embedding(chunk) for chunk in text_chunks]

# Function to execute bulk insert into PostgreSQL
def bulk_insert(cursor, table_name, rows):
    if rows:
        columns = rows[0].keys()
        values = [[row[col] for col in columns] for row in rows]
        insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES %s"
        execute_values(cursor, insert_query, values)

# Function to get the latest version of a document
def get_latest_version(cursor, company_id, user_id, file_path):
    logging.info("Getting latest version...")
    cursor.execute(
        "SELECT MAX(version) FROM shankara_embeddings WHERE company_id = %s AND user_id = %s AND file_path = %s",
        (company_id, user_id, file_path)
    )
    result = cursor.fetchone()
    return result[0] if result[0] is not None else 0

# Function to update versioned data
def update_versioned_data(cursor, company_id, user_id, file_path, text_chunks, embeddings, new_version):
    logging.info("Updating versioned data...")
    embeddings_data = []
    indices_data = []
    for i, (chunk, embedding) in enumerate(zip(text_chunks, embeddings)):
        embeddings_data.append({
            'company_id': company_id,
            'user_id': user_id,
            'file_path': file_path,
            'version': new_version,
            'chunk_index': i,
            'paragraph': chunk,
            'embedding': embedding
        })
        indices_data.append({
            'company_id': company_id,
            'user_id': user_id,
            'file_path': file_path,
            'version': new_version,
            'chunk_index': i,
            'paragraph': chunk,
            'text_chunk': chunk
        })
    bulk_insert(cursor, 'shankara_embeddings', embeddings_data)
    bulk_insert(cursor, 'shankara_indices', indices_data)

# Main function
def main():
    # Load environment variables
    try:
        with open('.env.json') as f:
            env = json.load(f)
    except FileNotFoundError as e:
        logging.error("Environment file not found: %s", e)
        raise

    # Load manifest file
    try:
        with open('../data/manifest.json') as f:
            manifest = json.load(f)
    except FileNotFoundError as e:
        logging.error("Manifest file not found: %s", e)
        raise

    db_config = {
        'dbname': env.get('PG_DBNAME'),
        'user': env.get('PG_USER'),
        'password': env.get('PG_PASSWORD'),
        'host': env.get('PG_HOST'),
        'port': env.get('PG_PORT')
    }
    
    company_id = manifest.get('company_id')
    user_id = manifest.get('user_id')
    file_path = "/Users/ctp1126/Project/Shankara/data/files"

    # Set OpenAI API, Pinecone API, and Pinecone environment variables
    os.environ["OPENAI_API_KEY"] = env.get('OPENAI_API_KEY')

    # Set up logging
    logging.basicConfig(
        filename=env.get('LOG_FILENAME'),
        filemode=env.get('LOG_FILEMODE'),  # Append to the log file
        format=env.get('LOG_FORMAT'),
        level=logging.INFO 
    )

    logging.info("Starting the application..")
    
    # Connect to the database
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
    except psycopg2.Error as e:
        logging.error("Unable to connect to the database: %s", e)
        raise

    try:
        # Read and process files using SimpleDirectoryReader with chunking
        reader = SimpleDirectoryReader(input_dir=file_path)
        logging.info("Reading files...")
        documents = reader.load_data()
        for doc in documents:
            text_chunks = chunk_text(doc.text, manifest.get('chunk_size'), manifest.get('chunk_overlap'))
            embeddings = create_embeddings(text_chunks, manifest.get('embedding_model'), manifest.get('embedding_batch_size'))
            logging.info(embeddings)
            
            # Get the latest version and increment it
            latest_version = get_latest_version(cursor, company_id, user_id, file_path)
            new_version = latest_version + 1

            # Update versioned data
            update_versioned_data(cursor, company_id, user_id, file_path, text_chunks, embeddings, new_version)
        
        # Commit the transaction
        conn.commit()
    except Exception as e:
        logging.error("An error occurred during processing: %s", e)
        conn.rollback()
        raise
    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()

if __name__ == '__main__':
    main()
