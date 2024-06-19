-- Table for storing embeddings with versioning
CREATE TABLE shankara_embeddings (
    id SERIAL PRIMARY KEY,
    company_id int,
    user_id int,
    file_path TEXT,
    version INT,
    chunk_index INT,
    paragraph TEXT,
    embedding VECTOR(1536) 
);

-- Table for storing index metadata with versioning
CREATE TABLE shankara_indices (
    id SERIAL PRIMARY KEY,
    company_id int,
    user_id int,
    file_path TEXT,
    version INT,
    chunk_index INT,
    paragraph TEXT,
    text_chunk TEXT
);


