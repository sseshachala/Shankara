-- Table for storing embeddings with versioning
CREATE TABLE embeddings (
    id SERIAL PRIMARY KEY,
    company_id TEXT,
    user_id TEXT,
    file_path TEXT,
    version INT,
    chunk_index INT,
    paragraph TEXT,
    embedding FLOAT8[]
);

-- Table for storing index metadata with versioning
CREATE TABLE indices (
    id SERIAL PRIMARY KEY,
    company_id TEXT,
    user_id TEXT,
    file_path TEXT,
    version INT,
    chunk_index INT,
    paragraph TEXT,
    text_chunk TEXT
);
