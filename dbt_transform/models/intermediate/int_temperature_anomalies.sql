{{
  config(
    materialized='view'
  )
}}

-- Intermediate model for anomaly detection
-- Uses only actual fields available in Kaggle dataset

with deduplicated_staging as (
    select *,
        row_number() over (
            partition by device_id, reading_timestamp 
            order by _dlt_id desc
        ) as rn
    from {{ ref('stg_raw_temperature_readings') }}
),

valid_readings as (
    select *
    from deduplicated_staging
    where rn = 1  -- Only latest record per device + timestamp
      and is_valid_record = true
      and temperature_celsius is not null
),

-- Calculate global temperature statistics
global_stats as (
    select
        avg(temperature_celsius) as global_mean_temp,
        stddev(temperature_celsius) as global_stddev_temp,
        count(*) as total_readings
    from valid_readings
),

-- Calculate per-device statistics
device_stats as (
    select
        device_id,
        avg(temperature_celsius) as device_mean_temp,
        stddev(temperature_celsius) as device_stddev_temp,
        count(*) as device_reading_count,
        min(temperature_celsius) as device_min_temp,
        max(temperature_celsius) as device_max_temp
    from valid_readings
    group by device_id
    having count(*) >= 5  -- Need at least 5 readings for device-specific stats
),

-- Calculate per-location statistics
location_stats as (
    select
        location,
        avg(temperature_celsius) as location_mean_temp,
        stddev(temperature_celsius) as location_stddev_temp,
        count(*) as location_reading_count
    from valid_readings
    where location is not null
    group by location
    having count(*) >= 10  -- Need at least 10 readings for location stats
),

-- Calculate per-environment statistics  
environment_stats as (
    select
        environment_type,
        avg(temperature_celsius) as environment_mean_temp,
        stddev(temperature_celsius) as environment_stddev_temp,
        count(*) as environment_reading_count
    from valid_readings
    where environment_type is not null
    group by environment_type
    having count(*) >= 5  -- Need at least 5 readings for environment stats
),

-- Calculate anomaly scores
anomaly_calculations as (
    select
        vr.*,
        gs.global_mean_temp,
        gs.global_stddev_temp,
        ds.device_mean_temp,
        ds.device_stddev_temp,
        ds.device_reading_count,
        ls.location_mean_temp,
        ls.location_stddev_temp,
        es.environment_mean_temp,
        es.environment_stddev_temp,
        
        -- Global anomaly score (Z-score)
        case 
            when gs.global_stddev_temp > 0 then
                abs(vr.temperature_celsius - gs.global_mean_temp) / gs.global_stddev_temp
            else 0
        end as global_z_score,
        
        -- Device-specific anomaly score
        case 
            when ds.device_stddev_temp > 0 and ds.device_reading_count >= 5 then
                abs(vr.temperature_celsius - ds.device_mean_temp) / ds.device_stddev_temp
            else null
        end as device_z_score,
        
        -- Location-specific anomaly score
        case 
            when ls.location_stddev_temp > 0 then
                abs(vr.temperature_celsius - ls.location_mean_temp) / ls.location_stddev_temp
            else null
        end as location_z_score,
        
        -- Environment-specific anomaly score
        case 
            when es.environment_stddev_temp > 0 then
                abs(vr.temperature_celsius - es.environment_mean_temp) / es.environment_stddev_temp
            else null
        end as environment_z_score

    from valid_readings vr
    cross join global_stats gs
    left join device_stats ds on vr.device_id = ds.device_id
    left join location_stats ls on vr.location = ls.location
    left join environment_stats es on vr.environment_type = es.environment_type
),

final as (
    select
        *,
        
        -- Determine anomaly flags using configurable threshold
        case 
            when global_z_score > {{ var('anomaly_threshold_multiplier', 3) }} then true
            else false
        end as is_global_anomaly,
        
        case 
            when device_z_score > ({{ var('anomaly_threshold_multiplier', 3) }} - 0.5) then true
            else false
        end as is_device_anomaly,
        
        case 
            when location_z_score > ({{ var('anomaly_threshold_multiplier', 3) }} - 0.5) then true
            else false
        end as is_location_anomaly,
        
        case 
            when environment_z_score > ({{ var('anomaly_threshold_multiplier', 3) }} - 0.5) then true
            else false
        end as is_environment_anomaly,
        
        -- Combined anomaly flag
        case 
            when global_z_score > {{ var('anomaly_threshold_multiplier', 3) }}
                or device_z_score > ({{ var('anomaly_threshold_multiplier', 3) }} - 0.5)
                or location_z_score > ({{ var('anomaly_threshold_multiplier', 3) }} - 0.5)
                or environment_z_score > ({{ var('anomaly_threshold_multiplier', 3) }} - 0.5)
            then true
            else false
        end as is_anomaly,
        
        -- Overall anomaly score (highest of all)
        greatest(
            coalesce(global_z_score, 0),
            coalesce(device_z_score, 0),
            coalesce(location_z_score, 0),
            coalesce(environment_z_score, 0)
        ) as anomaly_score

    from anomaly_calculations
)

select * from final