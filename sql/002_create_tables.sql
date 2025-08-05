-- Create tables for IoT Temperature Pipeline
-- Connect to iot_temperature_db before running this script

-- Create schema for organizing tables
CREATE SCHEMA IF NOT EXISTS iot_raw;
CREATE SCHEMA IF NOT EXISTS iot_processed;
CREATE SCHEMA IF NOT EXISTS iot_metadata;

-- Grant usage on schemas
GRANT USAGE ON SCHEMA iot_raw TO iot_user;
GRANT USAGE ON SCHEMA iot_processed TO iot_user;
GRANT USAGE ON SCHEMA iot_metadata TO iot_user;
GRANT CREATE ON SCHEMA iot_raw TO iot_user;
GRANT CREATE ON SCHEMA iot_processed TO iot_user;
GRANT CREATE ON SCHEMA iot_metadata TO iot_user;

-- Raw temperature readings table (staging area)
CREATE TABLE IF NOT EXISTS iot_raw.temperature_readings (
    id SERIAL PRIMARY KEY,
    device_id VARCHAR(50) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    temperature DECIMAL(5,2),
    humidity DECIMAL(5,2),
    battery_level DECIMAL(5,2),
    signal_strength DECIMAL(6,2),
    location VARCHAR(100),
    device_type VARCHAR(50),
    firmware_version VARCHAR(20),
    file_name VARCHAR(255) NOT NULL,
    row_number INTEGER NOT NULL,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(device_id, timestamp, file_name, row_number)
);

-- Processed temperature readings table (cleaned and validated data)
CREATE TABLE IF NOT EXISTS iot_processed.temperature_readings (
    id SERIAL PRIMARY KEY,
    device_id VARCHAR(50) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    temperature DECIMAL(5,2) NOT NULL,
    humidity DECIMAL(5,2),
    battery_level DECIMAL(5,2),
    signal_strength DECIMAL(6,2),
    location VARCHAR(100),
    device_type VARCHAR(50),
    firmware_version VARCHAR(20),
    is_anomaly BOOLEAN DEFAULT FALSE,
    anomaly_score DECIMAL(8,4),
    data_quality_score DECIMAL(3,2),
    processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_raw_id INTEGER REFERENCES iot_raw.temperature_readings(id),
    UNIQUE(device_id, timestamp)
);

-- Data quality issues table
CREATE TABLE IF NOT EXISTS iot_metadata.data_quality_issues (
    id SERIAL PRIMARY KEY,
    raw_record_id INTEGER REFERENCES iot_raw.temperature_readings(id),
    issue_type VARCHAR(100) NOT NULL,
    issue_description TEXT,
    field_name VARCHAR(50),
    invalid_value TEXT,
    severity VARCHAR(20) CHECK (severity IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
    detected_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Pipeline run metadata table
CREATE TABLE IF NOT EXISTS iot_metadata.pipeline_runs (
    id SERIAL PRIMARY KEY,
    run_id UUID NOT NULL UNIQUE,
    file_name VARCHAR(255) NOT NULL,
    start_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_timestamp TIMESTAMP,
    status VARCHAR(20) CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED', 'PARTIAL_SUCCESS')) DEFAULT 'RUNNING',
    total_records INTEGER,
    processed_records INTEGER,
    rejected_records INTEGER,
    anomalies_detected INTEGER,
    error_message TEXT,
    processing_duration_seconds INTEGER
);

-- File processing status table
CREATE TABLE IF NOT EXISTS iot_metadata.file_processing_status (
    id SERIAL PRIMARY KEY,
    file_name VARCHAR(255) NOT NULL UNIQUE,
    file_size_bytes BIGINT,
    file_hash VARCHAR(64),
    first_processed_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_processed_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processing_count INTEGER DEFAULT 1,
    status VARCHAR(20) CHECK (status IN ('PROCESSING', 'COMPLETED', 'FAILED', 'SKIPPED')) DEFAULT 'PROCESSING'
);

-- Grant permissions on tables
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA iot_raw TO iot_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA iot_processed TO iot_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA iot_metadata TO iot_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA iot_raw TO iot_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA iot_processed TO iot_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA iot_metadata TO iot_user;