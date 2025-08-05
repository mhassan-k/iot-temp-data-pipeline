{{
  config(
    materialized='table'
  )
}}

-- Pipeline summary statistics for monitoring and observability
-- Based on actual Kaggle IoT dataset structure

with load_level_stats as (
    select
        _dlt_load_id,
        min(dbt_processing_timestamp) as load_first_processed,
        max(dbt_processing_timestamp) as load_last_processed,
        count(*) as total_records,
        sum(case when is_valid_record then 1 else 0 end) as valid_records,
        sum(case when not is_valid_record then 1 else 0 end) as invalid_records,
        sum(case when is_anomaly then 1 else 0 end) as anomaly_records,
        avg(data_quality_score) as avg_data_quality_score,
        min(data_quality_score) as min_data_quality_score,
        max(data_quality_score) as max_data_quality_score,
        count(distinct device_id) as unique_devices,
        count(distinct location) as unique_locations,
        count(distinct environment_type) as unique_environments,
        min(reading_timestamp) as earliest_reading,
        max(reading_timestamp) as latest_reading
    from {{ ref('mart_temperature_readings') }}
    group by _dlt_load_id
),

device_level_stats as (
    select
        device_id,
        count(*) as total_readings,
        sum(case when is_anomaly then 1 else 0 end) as anomaly_count,
        avg(temperature_celsius) as avg_temperature,
        min(temperature_celsius) as min_temperature,
        max(temperature_celsius) as max_temperature,
        stddev(temperature_celsius) as temperature_stddev,
        avg(data_quality_score) as avg_quality_score,
        min(reading_timestamp) as first_reading,
        max(reading_timestamp) as last_reading,
        extract(epoch from (max(reading_timestamp) - min(reading_timestamp))) / 3600 as reading_span_hours,
        count(distinct date_trunc('day', reading_timestamp)) as active_days,
        count(distinct location) as locations_visited,
        count(distinct environment_type) as environments_recorded
    from {{ ref('mart_temperature_readings') }}
    group by device_id
),

location_level_stats as (
    select
        location,
        environment_type,
        count(*) as total_readings,
        count(distinct device_id) as unique_devices,
        avg(temperature_celsius) as avg_temperature,
        min(temperature_celsius) as min_temperature,
        max(temperature_celsius) as max_temperature,
        stddev(temperature_celsius) as temperature_stddev,
        sum(case when is_anomaly then 1 else 0 end) as anomaly_count,
        avg(data_quality_score) as avg_quality_score
    from {{ ref('mart_temperature_readings') }}
    where location is not null and environment_type is not null
    group by location, environment_type
),

overall_stats as (
    select
        current_timestamp as summary_generated_at,
        count(*) as total_processed_records,
        sum(case when is_valid_record then 1 else 0 end) as total_valid_records,
        sum(case when not is_valid_record then 1 else 0 end) as total_invalid_records,
        sum(case when is_anomaly then 1 else 0 end) as total_anomalies,
        
        -- Temperature statistics
        avg(temperature_celsius) as global_avg_temperature,
        min(temperature_celsius) as global_min_temperature,
        max(temperature_celsius) as global_max_temperature,
        stddev(temperature_celsius) as global_temperature_stddev,
        
        -- Quality metrics
        avg(data_quality_score) as global_avg_quality_score,
        min(data_quality_score) as global_min_quality_score,
        max(data_quality_score) as global_max_quality_score,
        
        -- Coverage metrics
        count(distinct device_id) as total_unique_devices,
        count(distinct location) as total_unique_locations,
        count(distinct environment_type) as total_environment_types,
        count(distinct _dlt_load_id) as total_load_batches,
        
        -- Time span
        min(reading_timestamp) as earliest_reading_timestamp,
        max(reading_timestamp) as latest_reading_timestamp,
        extract(epoch from (max(reading_timestamp) - min(reading_timestamp))) / 86400 as data_span_days,
        
        -- Environment breakdown
        sum(case when environment_type = 'Indoor' then 1 else 0 end) as indoor_readings,
        sum(case when environment_type = 'Outdoor' then 1 else 0 end) as outdoor_readings,
        sum(case when environment_type = 'Unknown' then 1 else 0 end) as unknown_environment_readings
        
    from {{ ref('mart_temperature_readings') }}
),

anomaly_analysis as (
    select
        'Global Anomalies' as anomaly_type,
        sum(case when is_global_anomaly then 1 else 0 end) as anomaly_count,
        avg(case when is_global_anomaly then global_z_score else null end) as avg_z_score
    from {{ ref('mart_temperature_readings') }}
    
    union all
    
    select
        'Device Anomalies' as anomaly_type,
        sum(case when is_device_anomaly then 1 else 0 end) as anomaly_count,
        avg(case when is_device_anomaly then device_z_score else null end) as avg_z_score
    from {{ ref('mart_temperature_readings') }}
    
    union all
    
    select
        'Location Anomalies' as anomaly_type,
        sum(case when is_location_anomaly then 1 else 0 end) as anomaly_count,
        avg(case when is_location_anomaly then location_z_score else null end) as avg_z_score
    from {{ ref('mart_temperature_readings') }}
    
    union all
    
    select
        'Environment Anomalies' as anomaly_type,
        sum(case when is_environment_anomaly then 1 else 0 end) as anomaly_count,
        avg(case when is_environment_anomaly then environment_z_score else null end) as avg_z_score
    from {{ ref('mart_temperature_readings') }}
),

final_summary as (
    select
        os.*,
        
        -- Calculate percentages
        round((os.total_valid_records::numeric / os.total_processed_records) * 100, 2) as valid_record_percentage,
        round((os.total_anomalies::numeric / os.total_processed_records) * 100, 2) as anomaly_percentage,
        round((os.indoor_readings::numeric / os.total_processed_records) * 100, 2) as indoor_percentage,
        round((os.outdoor_readings::numeric / os.total_processed_records) * 100, 2) as outdoor_percentage,
        
        -- Calculation metadata
        current_timestamp as calculated_at,
        '{{ run_started_at }}' as dbt_run_started_at,
        '{{ invocation_id }}' as dbt_invocation_id
        
    from overall_stats os
)

select * from final_summary