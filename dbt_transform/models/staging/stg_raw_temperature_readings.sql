{{
  config(
    materialized='view'
  )
}}

-- Staging model for raw temperature readings from Kaggle IoT dataset
-- Note: DLT has transformed the original Kaggle columns into standardized names
-- Original: id -> device_id, noted_date -> timestamp, temp -> temperature, room_id/id -> location

with source_data as (
    select * from {{ source('dlt_raw', 'raw_temperature_readings') }}
    where _dlt_id is not null  -- Ensure we have valid DLT records
),

cleaned_data as (
    select
        -- Use DLT's generated ID as record identifier
        _dlt_id as record_id,
        
        -- Device information (DLT transformed 'id' -> 'device_id')
        device_id,
        
        -- Timestamp (DLT transformed 'noted_date' -> 'timestamp')
        timestamp as reading_timestamp,
        
        -- Temperature (DLT transformed 'temp' -> 'temperature')
        case 
            when temperature is not null 
            and temperature between {{ var('min_temperature', -50) }} and {{ var('max_temperature', 100) }}
            then temperature::numeric(5,2)
            else null
        end as temperature_celsius,
        
        -- Location (DLT transformed 'room_id/id' -> 'location')
        coalesce(location, 'Unknown_Room') as location,
        
        -- Environment type (derived from location since 'out_in' was transformed)
        case 
            when location ilike '%_out%' or location ilike '%outdoor%' then 'Outdoor'
            when location ilike '%_in%' or location ilike '%indoor%' or location ilike '%office%' then 'Indoor'
            else 'Unknown'
        end as environment_type,
        
        -- DLT metadata
        _dlt_load_id,
        _dlt_id
        
    from source_data
),

final as (
    select
        record_id,
        device_id,
        reading_timestamp,
        temperature_celsius,
        location,
        environment_type,
        _dlt_load_id,
        _dlt_id,
        
        -- Data quality flags
        case 
            when device_id is null or trim(device_id) = '' then false
            when reading_timestamp is null then false
            when temperature_celsius is null then false
            else true
        end as is_valid_record,
        
        -- Quality score based on available core fields
        (
            case when device_id is not null and trim(device_id) != '' then 0.4 else 0 end +
            case when reading_timestamp is not null then 0.4 else 0 end +
            case when temperature_celsius is not null then 0.2 else 0 end
        ) as data_quality_score,
        
        -- Add processing timestamp
        current_timestamp as dbt_processing_timestamp
        
    from cleaned_data
)

select * from final