"""
DLT Pipeline for IoT Temperature Data Ingestion
"""
import os
import pandas as pd
from pathlib import Path
from typing import Iterator, Dict, Any, List
from datetime import datetime
import hashlib

import dlt
from dlt.sources.helpers import requests


@dlt.source
def iot_temperature_source(landing_zone_path: str = "./landing_zone") -> Iterator[dlt.resource]:
    """DLT source for IoT temperature data from CSV files"""
    
    @dlt.resource(
        name="raw_temperature_readings",
        write_disposition="append",
        primary_key="file_record_id"
    )
    def load_temperature_files() -> Iterator[Dict[str, Any]]:
        """Load and yield temperature readings from CSV files"""
        
        landing_zone = Path(landing_zone_path)
        
        # Find CSV files that need processing
        csv_files = list(landing_zone.glob("*.csv"))
        processed_files = _get_processed_files()
        
        for csv_file in csv_files:
            file_hash = _calculate_file_hash(csv_file)
            
            # Skip if already processed
            if file_hash in processed_files:
                print(f"⏭️  Skipping already processed file: {csv_file.name}")
                continue
            
            print(f"📄 Processing file: {csv_file.name}")
            
            try:
                # Load CSV with multiple encoding attempts
                df = _load_csv_with_fallback(csv_file)
                
                if df is None or df.empty:
                    print(f"⚠️  Empty or unreadable file: {csv_file.name}")
                    continue
                
                # Transform to standard format if needed
                df = _transform_to_standard_format(df, csv_file.name)
                
                # Add metadata
                file_metadata = {
                    'file_name': csv_file.name,
                    'file_path': str(csv_file),
                    'file_size_bytes': csv_file.stat().st_size,
                    'file_hash': file_hash,
                    'ingestion_timestamp': datetime.now(),
                    'total_records': len(df)
                }
                
                # Yield records with metadata
                for idx, row in df.iterrows():
                    record = row.to_dict()
                    
                    # Add unique identifier and metadata
                    record.update({
                        'file_record_id': f"{file_hash}_{idx}",
                        'row_number': idx + 1,
                        **file_metadata
                    })
                    
                    yield record
                    
                print(f"✅ Processed {len(df)} records from {csv_file.name}")
                
            except Exception as e:
                print(f"❌ Error processing {csv_file.name}: {e}")
                continue
    
    @dlt.resource(
        name="file_processing_log",
        write_disposition="append"
    )
    def log_file_processing() -> Iterator[Dict[str, Any]]:
        """Log file processing metadata"""
        
        landing_zone = Path(landing_zone_path)
        csv_files = list(landing_zone.glob("*.csv"))
        
        for csv_file in csv_files:
            yield {
                'file_name': csv_file.name,
                'file_path': str(csv_file),
                'file_size_bytes': csv_file.stat().st_size,
                'file_hash': _calculate_file_hash(csv_file),
                'file_modified_time': datetime.fromtimestamp(csv_file.stat().st_mtime),
                'processing_timestamp': datetime.now(),
                'status': 'processed'
            }
    
    return load_temperature_files(), log_file_processing()


def _load_csv_with_fallback(file_path: Path) -> pd.DataFrame:
    """Load CSV with multiple encoding fallbacks"""
    encodings = ['utf-8', 'utf-8-sig', 'latin1', 'cp1252', 'iso-8859-1']
    
    for encoding in encodings:
        try:
            df = pd.read_csv(file_path, encoding=encoding)
            print(f"✅ Loaded {file_path.name} with {encoding} encoding")
            return df
        except UnicodeDecodeError:
            continue
        except Exception as e:
            print(f"⚠️  Error with {encoding}: {e}")
            continue
    
    print(f"❌ Could not read {file_path.name} with any encoding")
    return None


def _transform_to_standard_format(df: pd.DataFrame, file_name: str) -> pd.DataFrame:
    """Transform various CSV formats to our standard schema"""
    
    # Check if it's already in our standard format
    expected_columns = {'device_id', 'timestamp', 'temperature'}
    if expected_columns.issubset(set(df.columns)):
        print(f"📋 File {file_name} already in standard format")
        return df
    
    # Check if it's Kaggle format
    kaggle_columns = {'id', 'room_id/id', 'noted_date', 'temp', 'out/in'}
    if kaggle_columns.issubset(set(df.columns)):
        print(f"🔄 Transforming Kaggle format: {file_name}")
        return _transform_kaggle_format(df)
    
    # If unknown format, try to map common column names
    print(f"🔍 Attempting to map unknown format: {file_name}")
    return _transform_generic_format(df)


def _transform_kaggle_format(df: pd.DataFrame) -> pd.DataFrame:
    """Transform Kaggle dataset format to our standard format"""
    
    transformed_df = pd.DataFrame()
    
    # Device ID from original ID
    transformed_df['device_id'] = df['id'].apply(
        lambda x: f"IOT_TEMP_{str(x).split('_')[-1][:8].upper()}" if pd.notna(x) else "IOT_TEMP_UNKNOWN"
    )
    
    # Timestamp conversion
    transformed_df['timestamp'] = pd.to_datetime(df['noted_date'], format='%d-%m-%Y %H:%M', errors='coerce')
    
    # Temperature
    transformed_df['temperature'] = df['temp'].astype(float)
    
    # Location
    transformed_df['location'] = df.apply(
        lambda row: f"{str(row['room_id/id']).replace('Room ', '').replace('Admin', 'Office')}_{str(row['out/in']).lower()}",
        axis=1
    )
    
    # Generate synthetic additional fields
    import numpy as np
    np.random.seed(42)
    n = len(transformed_df)
    
    # Humidity (correlated with temperature)
    base_humidity = np.where(
        transformed_df['location'].str.contains('in', case=False, na=False),
        45, 65  # Indoor vs outdoor base humidity
    )
    temp_effect = (transformed_df['temperature'] - 25) * -1.2
    humidity_noise = np.random.normal(0, 8, n)
    transformed_df['humidity'] = np.clip(base_humidity + temp_effect + humidity_noise, 20, 95).round(1)
    
    # Battery level
    base_battery = np.random.uniform(70, 100, n)
    time_decay = np.arange(n) * 0.001
    battery_noise = np.random.normal(0, 5, n)
    transformed_df['battery_level'] = np.clip(base_battery - time_decay + battery_noise, 10, 100).round(1)
    
    # Signal strength
    base_signal = np.where(
        transformed_df['location'].str.contains('in', case=False, na=False),
        -55, -45  # Indoor vs outdoor signal strength
    )
    signal_noise = np.random.normal(0, 10, n)
    transformed_df['signal_strength'] = np.clip(base_signal + signal_noise, -90, -20).round(1)
    
    # Device type and firmware
    device_types = ['DHT22', 'DS18B20', 'SHT30', 'BME280', 'TMP36']
    firmware_versions = ['v1.2.3', 'v1.2.4', 'v1.3.0', 'v1.3.1', 'v2.0.0']
    
    transformed_df['device_type'] = np.random.choice(device_types, n)
    transformed_df['firmware_version'] = np.random.choice(firmware_versions, n)
    
    # Remove invalid timestamps
    initial_count = len(transformed_df)
    transformed_df = transformed_df.dropna(subset=['timestamp'])
    if len(transformed_df) != initial_count:
        print(f"⚠️  Removed {initial_count - len(transformed_df)} records with invalid timestamps")
    
    return transformed_df


def _transform_generic_format(df: pd.DataFrame) -> pd.DataFrame:
    """Transform Kaggle CSV format - keep only actual columns"""
    
    print(f"📊 Processing DataFrame with columns: {list(df.columns)}")
    
    # Kaggle data has columns: id, room_id/id, noted_date, temp, out/in
    # We'll keep the original structure and let dbt handle the transformations
    
    # Clean up column names to be SQL-friendly
    new_df = df.copy()
    
    # Rename problematic column names
    if 'room_id/id' in new_df.columns:
        new_df = new_df.rename(columns={'room_id/id': 'room_id_id'})
    
    if 'out/in' in new_df.columns:
        new_df = new_df.rename(columns={'out/in': 'out_in'})
    
    print(f"✅ Cleaned column names: {list(new_df.columns)}")
    print(f"📈 Records to process: {len(new_df)}")
    
    return new_df


def _calculate_file_hash(file_path: Path) -> str:
    """Calculate SHA-256 hash of file"""
    hash_sha256 = hashlib.sha256()
    try:
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_sha256.update(chunk)
        return hash_sha256.hexdigest()
    except Exception:
        return f"error_{file_path.name}_{file_path.stat().st_size}"


def _get_processed_files() -> set:
    """Get list of already processed file hashes"""
    # TODO: Query database for processed files
    # For now, return empty set
    return set()


def run_iot_temperature_pipeline(landing_zone_path: str = "./landing_zone"):
    """Run the IoT temperature data ingestion pipeline"""
    
    print("🚀 Starting DLT IoT Temperature Data Ingestion Pipeline")
    print(f"📂 Landing zone: {landing_zone_path}")
    
    # Create the pipeline
    pipeline = dlt.pipeline(
        pipeline_name="iot_temperature_ingestion",
        destination="postgres",
        dataset_name="dlt_raw",
        progress="log"
    )
    
    # Load the source
    source = iot_temperature_source(landing_zone_path)
    
    # Run the pipeline
    load_info = pipeline.run(source)
    
    print(f"✅ Pipeline completed successfully!")
    print(f"📊 Load info: {load_info}")
    
    return load_info


if __name__ == "__main__":
    import sys
    
    landing_zone = sys.argv[1] if len(sys.argv) > 1 else "./landing_zone"
    run_iot_temperature_pipeline(landing_zone)