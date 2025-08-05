"""
Airflow 3.0 DAG for Modern IoT Temperature Data Pipeline
Uses dlt for ingestion and dbt for transformation
"""
from datetime import datetime, timedelta
from pathlib import Path
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator

# dbt Cosmos imports
from cosmos import DbtDag, DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig

# Configuration
DBT_PROJECT_PATH = Path("/opt/airflow/dbt_transform")
DBT_PROFILES_PATH = Path("/opt/airflow/dbt_transform/profiles")

default_args = {
    'owner': 'iot_pipeline',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

# dbt Cosmos configuration

# Use environment variable approach to avoid duplicate key issue
profile_config = ProfileConfig(
    profile_name="iot_temperature",
    target_name="prod",
    profiles_yml_filepath=DBT_PROFILES_PATH / "profiles.yml"
)

execution_config = ExecutionConfig(
    dbt_executable_path="/opt/airflow/.local/bin/dbt"
)

def download_kaggle_data(**context):
    """Download and prepare Kaggle IoT temperature dataset"""
    import kagglehub
    import shutil
    from pathlib import Path
    import pandas as pd
    
    print("🌡️ Downloading IoT Temperature Dataset from Kaggle...")
    print("=" * 60)
    
    try:
        # Download latest version
        path = kagglehub.dataset_download("atulanandjha/temperature-readings-iot-devices")
        print(f"✅ Dataset downloaded to: {path}")
        
        # List files in the downloaded dataset
        dataset_path = Path(path)
        files = list(dataset_path.glob("*.csv"))
        
        if not files:
            print("❌ No CSV files found in the dataset")
            raise Exception("No CSV files found in the dataset")
        
        print(f"📁 Found {len(files)} CSV file(s):")
        for file in files:
            size_mb = file.stat().st_size / (1024 * 1024)
            print(f"   - {file.name} ({size_mb:.2f} MB)")
        
        # Ensure landing zone exists
        landing_zone = Path("/opt/airflow/landing_zone")
        landing_zone.mkdir(exist_ok=True)
        
        # Copy files to landing zone with descriptive names
        copied_files = []
        for i, file in enumerate(files):
            # Create a descriptive filename
            new_name = f"kaggle_iot_temperature_{i+1:02d}_{file.name}"
            dest_path = landing_zone / new_name
            
            # Copy file
            shutil.copy2(file, dest_path)
            copied_files.append(dest_path)
            
            print(f"📋 Copied {file.name} → {new_name}")
            
            # Show sample data
            try:
                df = pd.read_csv(dest_path, nrows=5)
                print(f"   Sample data preview ({len(df)} rows shown):")
                print(f"   Columns: {list(df.columns)}")
                print(f"   Shape: {df.shape}")
                print()
            except Exception as e:
                print(f"   ⚠️ Could not preview data: {e}")
        
        print("✅ Kaggle dataset successfully prepared for pipeline!")
        print(f"📂 Files ready in landing zone: {len(copied_files)}")
        
        # Store file info in XCom for downstream tasks
        return {
            "files_downloaded": len(copied_files),
            "total_size_mb": sum(f.stat().st_size for f in copied_files) / (1024 * 1024),
            "file_names": [f.name for f in copied_files]
        }
        
    except Exception as e:
        print(f"❌ Error downloading or preparing dataset: {e}")
        raise

def run_dlt_ingestion(**context):
    """Run DLT pipeline for data ingestion"""
    import subprocess
    import os
    
    print("🚀 Starting DLT Data Ingestion Pipeline")
    
    # Set environment variables for DLT
    env = os.environ.copy()
    env.update({
        'DLT_POSTGRES_HOST': os.getenv('DB_HOST', 'postgres'),
        'DLT_POSTGRES_PORT': os.getenv('DB_PORT', '5432'),
        'DLT_POSTGRES_USERNAME': os.getenv('DB_USER', 'iot_user'),
        'DLT_POSTGRES_PASSWORD': os.getenv('DB_PASSWORD', 'iot_password'),
        'DLT_POSTGRES_DATABASE': os.getenv('DB_NAME', 'iot_temperature_db'),
    })
    
    try:
        # Run DLT pipeline
        result = subprocess.run([
            'python', '/opt/airflow/dlt_ingest/iot_temperature_pipeline.py', 
            '/opt/airflow/landing_zone'
        ], 
        env=env,
        capture_output=True,
        text=True,
        timeout=600  # 10 minute timeout
        )
        
        print(f"DLT Pipeline stdout: {result.stdout}")
        if result.stderr:
            print(f"DLT Pipeline stderr: {result.stderr}")
        
        if result.returncode != 0:
            raise Exception(f"DLT pipeline failed with return code {result.returncode}")
        
        print("✅ DLT Data Ingestion completed successfully")
        
        return {
            'status': 'success',
            'stdout': result.stdout,
            'ingestion_timestamp': datetime.now().isoformat()
        }
        
    except subprocess.TimeoutExpired:
        raise Exception("DLT pipeline timed out after 10 minutes")
    except Exception as e:
        print(f"❌ DLT pipeline failed: {e}")
        raise

def generate_pipeline_report(**context):
    """Generate comprehensive pipeline report"""
    
    # Get results from upstream tasks
    ingestion_result = context['task_instance'].xcom_pull(task_ids='dlt_ingest_data')
    
    print("=" * 60)
    print("🌡️  IoT TEMPERATURE PIPELINE EXECUTION REPORT")
    print("=" * 60)
    print(f"📅 Pipeline Run Date: {context['ds']}")
    print(f"⏰ Execution Time: {datetime.now()}")
    print()
    
    # DLT Ingestion Summary
    print("📥 DLT DATA INGESTION:")
    print(f"   Status: {ingestion_result.get('status', 'unknown')}")
    print(f"   Completed: {ingestion_result.get('ingestion_timestamp', 'N/A')}")
    print()
    
    # dbt models will be handled by Cosmos TaskGroup
    print("🔄 DBT TRANSFORMATION: Handled by Cosmos TaskGroup")
    print("   Individual model status available in Airflow UI")
    print()
    
    print("=" * 60)
    
    return {
        'pipeline_status': 'success',
        'ingestion_status': ingestion_result.get('status'),
        'report_generated_at': datetime.now().isoformat()
    }

# Create the main DAG
with DAG(
    'iot_temperature_pipeline',
    default_args=default_args,
    description='Modern IoT Temperature Pipeline with dlt + dbt Cosmos',
    schedule='*/30 * * * *',  # Run every 30 minutes
    max_active_runs=1,
    tags=['iot', 'temperature', 'dlt', 'dbt', 'cosmos', 'modern-data-stack', 'kaggle'],
    catchup=False,
) as dag:

    # Task 1: Download Kaggle dataset
    download_data = PythonOperator(
        task_id='download_kaggle_data',
        python_callable=download_kaggle_data,
    )

    # Task 2: Check for new files (optional, since we just downloaded)
    check_files = FileSensor(
        task_id='check_for_new_files',
        filepath='/opt/airflow/landing_zone',
        fs_conn_id='fs_default',
        poke_interval=30,
        timeout=120,  # Reduced timeout since we just downloaded data
        mode='poke',
    )

    # Task 2: Run DLT data ingestion
    dlt_ingest = PythonOperator(
        task_id='dlt_ingest_data',
        python_callable=run_dlt_ingestion,
    )

    # Task 3: dbt Cosmos TaskGroup for transformations
    dbt_tg = DbtTaskGroup(
        group_id="dbt_transformations",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=execution_config,
        operator_args={
            "install_deps": True,
            "full_refresh": True,
        },
    )

    # Task 4: Generate comprehensive report
    generate_report = PythonOperator(
        task_id='generate_pipeline_report',
        python_callable=generate_pipeline_report,
    )

    # Task 5: Cleanup old data (optional) - Using Python instead of PostgresOperator
    def cleanup_old_data(**context):
        """Cleanup old data and update table statistics"""
        import psycopg2
        import os
        
        conn_params = {
            'host': os.getenv('DB_HOST', 'postgres'),
            'port': int(os.getenv('DB_PORT', 5432)),
            'database': os.getenv('DB_NAME', 'iot_temperature_db'),
            'user': os.getenv('DB_USER', 'iot_user'),
            'password': os.getenv('DB_PASSWORD', 'iot_password')
        }
        
        try:
            with psycopg2.connect(**conn_params) as conn:
                with conn.cursor() as cur:
                    # Archive data older than 90 days
                    print("🗑️ Cleaning up old data...")
                    cur.execute("""
                        DELETE FROM dlt_raw.raw_temperature_readings 
                        WHERE ingestion_timestamp < CURRENT_DATE - INTERVAL '90 days'
                    """)
                    deleted_rows = cur.rowcount
                    print(f"✅ Deleted {deleted_rows} old records")
                    
                    # Update table statistics
                    print("📊 Updating table statistics...")
                    tables_to_analyze = [
                        'dbt_staging.stg_raw_temperature_readings',
                        'dbt_marts.mart_temperature_readings', 
                        'dbt_marts.mart_pipeline_summary'
                    ]
                    
                    for table in tables_to_analyze:
                        try:
                            cur.execute(f"ANALYZE {table}")
                            print(f"✅ Analyzed {table}")
                        except Exception as e:
                            print(f"⚠️ Could not analyze {table}: {e}")
                    
                    conn.commit()
                    print("✅ Cleanup completed successfully!")
                    
        except Exception as e:
            print(f"❌ Cleanup failed: {e}")
            raise

    cleanup_old_data = PythonOperator(
        task_id='cleanup_old_data',
        python_callable=cleanup_old_data,
    )

    # Task 6: Cleanup logs
    cleanup_logs = BashOperator(
        task_id='cleanup_old_logs',
        bash_command='find /opt/airflow/logs -name "*.log" -mtime +7 -delete || true',
    )

    # Set task dependencies - Modern ELT workflow with Cosmos
    download_data >> check_files >> dlt_ingest >> dbt_tg >> generate_report >> [cleanup_old_data, cleanup_logs]