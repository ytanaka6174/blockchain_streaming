-- Create the Raw schema to ingest events
CREATE SCHEMA IF NOT EXISTS RAW;

-- Main events table for blockchain events
CREATE TABLE IF NOT EXISTS raw.events (
    id SERIAL PRIMARY KEY,
    contract_address TEXT NOT NULL,
    block_number BIGINT NOT NULL,
    transaction_hash TEXT NOT NULL,
    log_index INT NOT NULL,
    event_signature TEXT,
    topics TEXT,
    raw_data TEXT,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (transaction_hash, log_index)
);
