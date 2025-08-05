{{
  config(
    materialized='table',
    indexes=[
      {'columns': ['device_id', 'reading_timestamp'], 'unique': false},
      {'columns': ['reading_timestamp'], 'unique': false},
      {'columns': ['location'], 'unique': false},
      {'columns': ['environment_type'], 'unique': false},
      {'columns': ['is_anomaly'], 'unique': false}
    ]
  )
}}

-- Final mart for processed temperature readings from Kaggle IoT dataset
-- Clean, validated, and enriched data ready for analytics
-- Deduplication handled in intermediate layer

with enriched_readings as (
    select
        -- Primary identifiers
        record_id,
        device_id,
        reading_timestamp,
        
        -- Measurements (only temperature is available in Kaggle data)
        temperature_celsius,
        
        -- Location and environment info  
        location,
        environment_type,
        
        -- Data quality metrics
        data_quality_score,
        is_valid_record,
        
        -- Anomaly detection results
        is_anomaly,
        is_global_anomaly,
        is_device_anomaly,
        is_location_anomaly,
        is_environment_anomaly,
        anomaly_score,
        global_z_score,
        device_z_score,
        location_z_score,
        environment_z_score,
        
        -- Statistical context
        global_mean_temp,
        global_stddev_temp,
        device_mean_temp,
        device_stddev_temp,
        location_mean_temp,
        location_stddev_temp,
        environment_mean_temp,
        environment_stddev_temp,
        
        -- DLT metadata
        _dlt_load_id,
        _dlt_id,
        dbt_processing_timestamp
        
    from {{ ref('int_temperature_anomalies') }}
),

-- Add derived insights
insights as (
    select
        *,
        
        -- Temperature categories
        case 
            when temperature_celsius < 0 then 'Freezing'
            when temperature_celsius between 0 and 15 then 'Cold'
            when temperature_celsius between 16 and 25 then 'Comfortable'
            when temperature_celsius between 26 and 35 then 'Warm'
            when temperature_celsius > 35 then 'Hot'
            else 'Unknown'
        end as temperature_category,
        
        -- Time-based dimensions
        extract(hour from reading_timestamp) as reading_hour,
        extract(dow from reading_timestamp) as reading_day_of_week,
        date(reading_timestamp) as reading_date,
        extract(month from reading_timestamp) as reading_month,
        extract(year from reading_timestamp) as reading_year,
        
        -- Environment comparison
        case 
            when environment_type = 'Indoor' and temperature_celsius < 15 then 'Cold_Indoor'
            when environment_type = 'Indoor' and temperature_celsius > 30 then 'Hot_Indoor'
            when environment_type = 'Outdoor' and temperature_celsius < 0 then 'Freezing_Outdoor'
            when environment_type = 'Outdoor' and temperature_celsius > 40 then 'Extreme_Heat_Outdoor'
            else 'Normal'
        end as environment_condition,
        
        -- Data freshness
        case 
            when reading_timestamp >= current_date - interval '1 day' then 'Recent'
            when reading_timestamp >= current_date - interval '7 days' then 'This_Week'
            when reading_timestamp >= current_date - interval '30 days' then 'This_Month'
            else 'Historical'
        end as data_freshness

    from enriched_readings
)

select * from insights