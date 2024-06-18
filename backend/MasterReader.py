import os
import json
import psycopg2
from psycopg2.extras import execute_values
from llama_index.core import SimpleDirectoryReader

from llama_index.embeddings.openai import OpenAIEmbedding
from llama_index.core import Settings
# Ensure you have the appropriate library for OpenAI embeddings


# Configuration
storage_directory = '../data'
db_config = {
    'dbname': '',
    'user': '',
    'password': '',
    'host': '',
    'port': 25060
}

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
    print("Creating embeddings... for ", model)
    print("With batch size ", batch_size)
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
    print("Getting latest version...")
    cursor.execute(
        "SELECT MAX(version) FROM embeddings WHERE company_id = %s AND user_id = %s AND file_path = %s",
        (company_id, user_id, file_path)
    )
    result = cursor.fetchone()
    return result[0] if result[0] is not None else 0

# Function to update versioned data
def update_versioned_data(cursor, company_id, user_id, file_path, text_chunks, embeddings, new_version):
    print("Updating versioned data...")
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
    bulk_insert(cursor, 'embeddings', embeddings_data)
    bulk_insert(cursor, 'indices', indices_data)

# Main function
def main():
    # Connect to the database
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    # Load manifest file
    with open('../data/manifest.json') as f:
        manifest = json.load(f)
    
    company_id = manifest.get('company_id')
    user_id = manifest.get('user_id')
    file_path = "/Users/ctp1126/Project/Shankara/data/files"
    #manifest.get('data_folder')

    # Set OpenAI API key
    os.environ["OPENAI_API_KEY"] = "sk-"

    # Read and process files using SimpleDirectoryReader with chunking
    reader = SimpleDirectoryReader(input_dir=file_path)
    print("Reading files...")
    documents = reader.load_data()
    for doc in documents:
        text_chunks = chunk_text(doc.text, manifest.get('chunk_size'), manifest.get('chunk_overlap'))
        embeddings = create_embeddings(text_chunks, manifest.get('embedding_model'), manifest.get('embedding_batch_size'))
        
        # Get the latest version and increment it
        latest_version = get_latest_version(cursor, company_id, user_id, file_path)
        new_version = latest_version + 1

        # Update versioned data
        update_versioned_data(cursor, company_id, user_id, file_path, text_chunks, embeddings, new_version)

    # Commit the transaction and close the connection
    conn.commit()
    cursor.close()
    conn.close()


if __name__ == '__main__':
    main()
