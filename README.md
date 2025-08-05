# Enterprise IoT Temperature Data Pipeline

A production-ready, scalable data pipeline for processing IoT temperature sensor data using modern data stack technologies. This solution provides automated data ingestion, transformation, quality validation, and anomaly detection for enterprise IoT deployments.

## Overview

This pipeline implements a modern ELT (Extract, Load, Transform) architecture that processes temperature readings from IoT sensors, performs comprehensive data validation and quality checks, and delivers analytics-ready datasets with built-in anomaly detection capabilities.

## Architecture & Technology Stack

**Core Technologies:**
- 📥 **dlt (Data Load Tool)** - Schema-aware data ingestion with automatic type inference and lineage tracking
- 🔄 **dbt (Data Build Tool)** - SQL-based transformations with built-in testing, documentation, and lineage
- 🌌 **Astronomer Cosmos** - Native dbt integration providing task-level orchestration in Airflow
- 📅 **Apache Airflow** - Workflow orchestration with enterprise-grade scheduling and monitoring

## Data Flow Architecture


### Pipeline Execution Flow

The pipeline follows a structured ELT workflow with the following stages:

1. **Data Acquisition** - Automated download of IoT temperature datasets from external sources
2. **Data Ingestion** - Schema-aware loading into PostgreSQL with full lineage tracking via dlt
3. **Data Transformation** - Multi-stage SQL transformations using dbt:
   - **Staging Layer**: Data cleaning, validation, and standardization
   - **Intermediate Layer**: Business logic application and anomaly detection
   - **Marts Layer**: Analytics-ready datasets with comprehensive metrics

### Data Layer Architecture

**Raw Layer** (`dlt_raw` schema) - Immutable source data with full lineage
- `raw_temperature_readings` - Original ingested data with metadata
- `file_processing_log` - Processing audit trail and metadata tracking

**Staging Layer** (`dbt_staging` schema) - Cleaned and validated data
- `stg_raw_temperature_readings` - Standardized, validated temperature readings

**Intermediate Layer** (`dbt_intermediate` schema) - Business logic and analytics
- `int_temperature_anomalies` - Statistical anomaly detection and scoring

**Marts Layer** (`dbt_marts` schema) - Production-ready analytical datasets
- `mart_temperature_readings` - Final analytics-ready temperature data
- `mart_pipeline_summary` - Pipeline execution metrics and observability

## Deployment Guide

### System Requirements

- Docker Engine 20.0+ and Docker Compose v2
- Minimum 4GB RAM (8GB recommended for production)
- Available ports: 5432 (PostgreSQL), 8080 (Airflow Web UI)
- Operating System: Linux, macOS, or Windows with WSL2

### Installation Steps

#### 1. Repository Setup
```bash
git clone git@github.com:mhassan-k/iot-temp-data-pipeline.git
cd iot-temp-data-pipeline
```

#### 2. Automated Data Acquisition
The pipeline includes automated download of production IoT temperature datasets (97,606 records from enterprise building sensors). No manual data preparation is required - the system handles data acquisition as part of the workflow execution.

#### 3. Service Deployment
```bash
# Recommended: Use the provided management script
./run-pipeline.sh start
```

### Service Access

#### Database Access
**PostgreSQL Database** (`localhost:5432`)
- Database: `iot_temperature_db`
- Username: `iot_user`  
- Password: `iot_password`

#### Workflow Management
**Apache Airflow Web UI** (`http://localhost:8080`)
- Username: `admin`
- Password: `admin`
- Primary DAG: `iot_temperature_pipeline`

## Pipeline Execution

### Workflow Execution Steps

1. **Access Airflow Web UI**: Navigate to `http://localhost:8080` (credentials: admin/admin)
2. **Locate Pipeline DAG**: Identify `iot_temperature_pipeline` in the DAG list
3. **Trigger Execution**: Initiate pipeline run via the trigger button
4. **Monitor Progress**: Observe real-time task execution and status



## Data Analysis & Monitoring

### Pipeline Monitoring Commands

```bash
# Service log analysis
docker-compose logs airflow_webserver
docker-compose logs airflow_scheduler

# Data volume verification
docker-compose exec postgres psql -U iot_user -d iot_temperature_db -c "
SELECT COUNT(*) as total_records FROM dbt_marts.mart_temperature_readings;
"

# Pipeline execution summary
docker-compose exec postgres psql -U iot_user -d iot_temperature_db -c "
SELECT * FROM dbt_marts.mart_pipeline_summary ORDER BY file_first_processed DESC;
"

# Anomaly detection results
docker-compose exec postgres psql -U iot_user -d iot_temperature_db -c "
SELECT device_id, COUNT(*) as anomaly_count 
FROM dbt_marts.mart_temperature_readings 
WHERE is_anomaly = true 
GROUP BY device_id 
ORDER BY anomaly_count DESC;
"
```

### Database Connection Parameters
- **Host**: `localhost:5432`
- **Database**: `iot_temperature_db`  
- **Username**: `iot_user`
- **Password**: `iot_password`

## Data Quality and Validation

### Validation Levels

1. **Schema Validation**: Required fields, data types
2. **Range Validation**: Realistic value ranges
3. **Business Logic**: Domain-specific rules
4. **Anomaly Detection**: Statistical outlier detection


## Monitoring and Observability

### Pipeline Metadata

Every pipeline run tracks:
- Processing start/end times
- Records processed/rejected
- Anomalies detected
- Error details
- Performance metrics

### Logs

- **Application Logs**: `/logs/pipeline.log`
- **Airflow Logs**: Available in Airflow UI
- **Database Logs**: PostgreSQL container logs

### Query Examples

```sql
-- Pipeline summary and data quality overview
SELECT 
    total_processed_records,
    total_valid_records,
    total_anomalies,
    round(valid_record_percentage, 2) as quality_pct,
    round(anomaly_percentage, 2) as anomaly_pct,
    total_unique_devices,
    total_unique_locations,
    earliest_reading_timestamp,
    latest_reading_timestamp
FROM dbt_staging_marts.mart_pipeline_summary;

-- Temperature readings analysis by device
SELECT 
    device_id,
    location,
    environment_type,
    COUNT(*) as total_readings,
    ROUND(AVG(temperature_celsius), 2) as avg_temp,
    ROUND(MIN(temperature_celsius), 2) as min_temp,
    ROUND(MAX(temperature_celsius), 2) as max_temp,
    SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) as anomalies,
    ROUND(AVG(anomaly_score), 3) as avg_anomaly_score,
    ROUND(AVG(data_quality_score), 3) as avg_quality_score
FROM dbt_staging_marts.mart_temperature_readings 
GROUP BY device_id, location, environment_type
ORDER BY anomalies DESC, total_readings DESC;

-- Environment-based temperature analysis
SELECT 
    environment_type,
    temperature_category,
    COUNT(*) as reading_count,
    ROUND(AVG(temperature_celsius), 2) as avg_temperature,
    COUNT(DISTINCT device_id) as unique_devices,
    COUNT(DISTINCT location) as unique_locations
FROM dbt_staging_marts.mart_temperature_readings 
GROUP BY environment_type, temperature_category
ORDER BY environment_type, avg_temperature;

-- Anomaly detection breakdown
SELECT 
    'Global Anomalies' as anomaly_type,
    SUM(CASE WHEN is_global_anomaly THEN 1 ELSE 0 END) as count,
    ROUND(AVG(CASE WHEN is_global_anomaly THEN global_z_score END), 3) as avg_z_score
FROM dbt_staging_marts.mart_temperature_readings
UNION ALL
SELECT 
    'Device Anomalies' as anomaly_type,
    SUM(CASE WHEN is_device_anomaly THEN 1 ELSE 0 END) as count,
    ROUND(AVG(CASE WHEN is_device_anomaly THEN device_z_score END), 3) as avg_z_score
FROM dbt_staging_marts.mart_temperature_readings
UNION ALL
SELECT 
    'Location Anomalies' as anomaly_type,
    SUM(CASE WHEN is_location_anomaly THEN 1 ELSE 0 END) as count,
    ROUND(AVG(CASE WHEN is_location_anomaly THEN location_z_score END), 3) as avg_z_score
FROM dbt_staging_marts.mart_temperature_readings
ORDER BY count DESC;

-- Recent temperature trends (last 24 hours if data available)
SELECT 
    reading_hour,
    environment_type,
    COUNT(*) as readings,
    ROUND(AVG(temperature_celsius), 2) as avg_temp,
    COUNT(DISTINCT device_id) as active_devices
FROM dbt_staging_marts.mart_temperature_readings 
WHERE data_freshness = 'Recent'
GROUP BY reading_hour, environment_type
ORDER BY reading_hour, environment_type;

-- Data quality assessment
SELECT 
    CASE 
        WHEN data_quality_score >= 0.8 THEN 'High Quality'
        WHEN data_quality_score >= 0.6 THEN 'Medium Quality'
        ELSE 'Low Quality'
    END as quality_tier,
    COUNT(*) as record_count,
    ROUND(AVG(data_quality_score), 3) as avg_score,
    COUNT(DISTINCT device_id) as devices_affected
FROM dbt_staging_marts.mart_temperature_readings
GROUP BY quality_tier
ORDER BY avg_score DESC;
```

## License

This project is licensed under the MIT License - see the LICENSE file for complete terms and conditions.

## Support

For technical support, feature requests, or general inquiries, please submit an issue through the project repository. Our team provides comprehensive assistance for deployment, configuration, and operational questions.
