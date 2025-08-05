-- Create database and user for IoT Temperature Pipeline
-- This script should be run by a PostgreSQL superuser

-- Create database
CREATE DATABASE iot_temperature_db;

-- Create user
CREATE USER iot_user WITH PASSWORD 'iot_password';

-- Grant privileges
GRANT ALL PRIVILEGES ON DATABASE iot_temperature_db TO iot_user;
GRANT CREATE ON DATABASE iot_temperature_db TO iot_user;