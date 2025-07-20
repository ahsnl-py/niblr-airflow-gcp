"""
Generic Scrap Job Pipeline - Simplified
This DAG provides a simple service for scraping data from API and uploading to GCS/BigQuery.
"""

import datetime
import os
import json
import pandas as pd
import time
import requests
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.utils.dates import days_ago
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from typing import Dict, Any, Optional


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

@dag(
    dag_id="generic_scrap_job",
    default_args=default_args,
    description="Simple scraping service for API data extraction and upload",
    schedule_interval=None,
    catchup=False,
    tags=['scraping', 'api', 'generic', 'data-pipeline'],
    params={
        'job_type': None,  # Required: Type of job (e.g., 'real_estate', 'job_listings')
        'config_feile': None,  # Optional: Path to config file (defaults to job_type.json)
        'api_base_url': 'http://host.docker.internal:8001/api/v1',  # API base URL
        'gcs_bucket': None,  # GCS bucket for uploads
        'bigquery_dataset': None,  # BigQuery dataset
        'bigquery_table': None,  # BigQuery table
    }
)
def generic_scrap_job():

    @task
    def load_job_config(**context):
        """
        Load simplified job configuration
        """
        params = context['params']
        job_type = params.get('job_type')
        
        if not job_type:
            raise ValueError("job_type parameter is required")
        
        # Determine config file path
        config_file = params.get('config_file')
        if not config_file:
            config_file = f"include/configs/{job_type}.json"
        
        print(f"Loading configuration for job_type: {job_type}")
        print(f"Config file path: {config_file}")
        
        try:
            # Load configuration from file
            with open(config_file, 'r') as f:
                job_config = json.load(f)
            
            # Validate required configuration fields
            required_fields = ['job_payload', 'output_config']
            missing_fields = [field for field in required_fields if field not in job_config]
            
            if missing_fields:
                raise ValueError(f"Missing required configuration fields: {missing_fields}")
            
            # Override with DAG parameters if provided
            if params.get('api_base_url'):
                job_config['api_base_url'] = params['api_base_url']
            if params.get('gcs_bucket'):
                job_config['output_config']['gcs_bucket'] = params['gcs_bucket']
            if params.get('bigquery_dataset'):
                job_config['output_config']['bigquery_dataset'] = params['bigquery_dataset']
            if params.get('bigquery_table'):
                job_config['output_config']['bigquery_table'] = params['bigquery_table']
            
            print(f"✅ Configuration loaded successfully")
            print(f"Job payload: {job_config['job_payload']}")
            print(f"Output config: {job_config['output_config']}")
            
            # Store configuration in XCom
            context['task_instance'].xcom_push(key='job_config', value=job_config)
            
            return job_config
            
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file not found: {config_file}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in configuration file: {str(e)}")
        except Exception as e:
            raise Exception(f"Error loading configuration: {str(e)}")

    @task
    def fetch_api_data(**context):
        """
        Fetch data from API with job creation and polling
        """
        job_config = context['task_instance'].xcom_pull(task_ids='load_job_config', key='job_config')
        
        api_base_url = job_config['api_base_url']
        job_payload = job_config['job_payload']
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        
        try:
            # Step 1: Create job
            print(f"Creating job with payload: {job_payload}")
            create_response = requests.post(
                f"{api_base_url}/jobs",
                headers=headers,
                json=job_payload,
                timeout=30
            )
            create_response.raise_for_status()
            
            job_data = create_response.json()
            job_id = job_data['id']
            print(f"Job created with ID: {job_id}")
            
            # Step 2: Poll for job completion
            max_attempts = 60
            delay_seconds = 5
            attempt = 0
            
            while attempt < max_attempts:
                print(f"Polling job status (attempt {attempt + 1}/{max_attempts})...")
                
                # Get job status
                status_response = requests.get(
                    f"{api_base_url}/jobs/{job_id}",
                    headers=headers,
                    timeout=30
                )
                status_response.raise_for_status()
                
                job_status = status_response.json()
                status = job_status['status']
                
                print(f"Job status: {status}")
                
                if status == 'completed':
                    print("Job completed successfully!")
                    break
                elif status == 'failed':
                    error_msg = job_status.get('error_message', 'Unknown error')
                    raise Exception(f"Job failed: {error_msg}")
                elif status in ['pending', 'running']:
                    print(f"Job still {status}, waiting {delay_seconds} seconds...")
                    time.sleep(delay_seconds)
                    attempt += 1
                else:
                    raise Exception(f"Unknown job status: {status}")
            
            if attempt >= max_attempts:
                raise Exception("Job polling timeout - job did not complete within expected time")
            
            # Step 3: Get job results
            print("Fetching job results...")
            result_response = requests.get(
                f"{api_base_url}/jobs/{job_id}/result",
                headers=headers,
                timeout=30
            )
            result_response.raise_for_status()
            
            result_data = result_response.json()
            extracted_data = result_data.get('data', [])
            
            print(f"Extracted {len(extracted_data)} items")
            print(f"Total items: {result_data.get('total_items', 0)}")
            print(f"Extraction time: {result_data.get('extraction_time', 0)} seconds")
            
            # Store the extracted data in XCom
            context['task_instance'].xcom_push(key='api_data', value=extracted_data)
            context['task_instance'].xcom_push(key='job_metadata', value=result_data.get('metadata', {}))
            
            return extracted_data
            
        except requests.exceptions.RequestException as e:
            print(f"Error in API request: {str(e)}")
            raise
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON response: {str(e)}")
            raise
        except Exception as e:
            print(f"Error in fetch_api_data: {str(e)}")
            raise

    @task
    def save_to_csv(**context):
        """
        Save API data directly to CSV with simple processing
        """
        job_config = context['task_instance'].xcom_pull(task_ids='load_job_config', key='job_config')
        api_data = context['task_instance'].xcom_pull(task_ids='fetch_api_data', key='api_data')
        
        output_config = job_config['output_config']
        
        try:
            # Convert to pandas DataFrame
            df = pd.DataFrame(api_data)
            
            if not df.empty:
                # Simple data cleaning - just remove completely empty rows
                df = df.dropna(how='all')
                
                # Add timestamp
                df['processed_at'] = datetime.datetime.now().isoformat()
                df['job_type'] = job_config['job_payload']['job_type']
                
                print(f"DataFrame shape: {df.shape}")
                print(f"DataFrame columns: {list(df.columns)}")
                print(f"First few rows:\n{df.head()}")
            
            # Create folder structure
            current_date = datetime.datetime.now()
            year_month = current_date.strftime("%Y-%m")
            
            # Create directory structure
            base_dir = "include/datasets"
            year_month_dir = os.path.join(base_dir, year_month)
            
            # Create directories if they don't exist
            os.makedirs(year_month_dir, exist_ok=True)
            
            # Generate filename
            timestamp = current_date.strftime("%Y%m%d_%H%M%S")
            job_type = job_config['job_payload']['job_type']
            filename = f"{job_type}_{timestamp}.csv"
            filepath = os.path.join(year_month_dir, filename)
            
            # Save DataFrame to CSV
            df.to_csv(filepath, index=False, encoding='utf-8')
            
            print(f"Data saved successfully to: {filepath}")
            print(f"File size: {os.path.getsize(filepath)} bytes")
            print(f"Records saved: {len(df)}")
            
            # Store filepath and metadata in XCom
            context['task_instance'].xcom_push(key='csv_filepath', value=filepath)
            context['task_instance'].xcom_push(key='year_month', value=year_month)
            
            return filepath
            
        except Exception as e:
            print(f"Error saving CSV file: {str(e)}")
            raise

    @task
    def trigger_upload_to_gcs_and_bigquery(**context):
        """
        Trigger the generic upload DAG to upload CSV file to GCS and load to BigQuery
        """
        job_config = context['task_instance'].xcom_pull(task_ids='load_job_config', key='job_config')
        csv_filepath = context['task_instance'].xcom_pull(task_ids='save_to_csv', key='csv_filepath')
        year_month = context['task_instance'].xcom_pull(task_ids='save_to_csv', key='year_month')
        
        output_config = job_config['output_config']
        
        # Build GCS destination path
        job_type = job_config['job_payload']['job_type']
        gcs_destination = f"{job_type}_raw/{year_month}/"
        
        # Configuration for the upload DAG
        upload_config = {
            'source_file_path': csv_filepath,
            'gcs_bucket': output_config['gcs_bucket'],
            'gcs_destination': gcs_destination,
            'mime_type': 'text/csv',
            'gzip': True,
            'bigquery_dataset_id': output_config['bigquery_dataset'],
            'bigquery_table_name': output_config['bigquery_table'],
            'bigquery_schema_name': output_config.get('bigquery_schema', output_config['bigquery_dataset']),
            'create_dataset_if_missing': True,
            'source_format': 'CSV',
            'write_disposition': 'WRITE_TRUNCATE',
            'autodetect': True,
            'skip_leading_rows': 1,
            'metadata': {
                'source': 'api',
                'job_type': job_type,
                'upload_date': '{{ ds }}',
                'year_month': year_month
            }
        }
        
        print(f"Upload configuration prepared:")
        for key, value in upload_config.items():
            print(f"  {key}: {value}")
        
        # Trigger the generic upload DAG
        trigger_upload = TriggerDagRunOperator(
            task_id='trigger_upload_to_gcs_and_bigquery',
            trigger_dag_id='upload_file_gc_job',
            conf=upload_config,
            wait_for_completion=True,
            poke_interval=30
        )
        
        # Execute the trigger
        result = trigger_upload.execute(context)
        
        print(f"✅ Upload DAG triggered successfully")
        print(f"📊 Trigger result: {result}")
        
        return result

    # Define task dependencies
    config = load_job_config()
    api_data = fetch_api_data()
    csv_file = save_to_csv()
    upload_result = trigger_upload_to_gcs_and_bigquery()
    
    # Set up the workflow
    config >> api_data >> csv_file >> upload_result

# Create the DAG instance
generic_scrap_job_instance = generic_scrap_job() 