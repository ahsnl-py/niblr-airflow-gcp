"""
Property API Data Pipeline using HttpOperator
This DAG fetches property data from an API, processes it, saves to CSV, and triggers upload to GCS/BigQuery.
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
    dag_id="property_api_pipeline",
    default_args=default_args,
    description="Fetch property data from API, save to CSV, and trigger upload to GCS/BigQuery",
    schedule_interval=None,
    catchup=False,
    tags=['property_listing', 'api', 'csv', 'upload'],
)
def property_api_pipeline():

    @task
    def fetch_api_data(**context):
        """
        Fetch data from property API with job creation and polling
        """
        # API configuration
        api_base_url = "http://host.docker.internal:8001/api/v1"
        # api_base_url = "http://localhost:8001/api/v1"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        
        # Job creation payload
        job_payload = {
            "job_type": "real_estate",
            "ai_model_provider": "groq",
            "storage_type": "memory",
            "session_id": "string",
            "metadata": {},
            "max_pages": 2
        }
        
        try:
            # Step 1: Create job
            print("Creating job...")
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
            max_attempts = 60  # Maximum 5 minutes (60 * 5 seconds)
            attempt = 0
            delay_seconds = 5
            
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
    def parse_and_process_data(**context):
        """
        Parse API data and load into pandas DataFrame
        """
        # Get data from previous task
        api_data = context['task_instance'].xcom_pull(task_ids='fetch_api_data', key='api_data')
        
        try:
            # Convert to pandas DataFrame
            df = pd.DataFrame(api_data)
            
            # Basic data cleaning
            if not df.empty:
                # Remove any completely empty rows
                df = df.dropna(how='all')
                
                # Fill NaN values with appropriate defaults
                df = df.fillna('')
                
                # Add timestamp
                df['processed_at'] = datetime.datetime.now().isoformat()
                
                print(f"DataFrame shape: {df.shape}")
                print(f"DataFrame columns: {list(df.columns)}")
                print(f"First few rows:\n{df.head()}")
            
            # Store DataFrame in XCom (as JSON for compatibility)
            df_json = df.to_json(orient='records', date_format='iso')
            context['task_instance'].xcom_push(key='processed_data', value=df_json)
            
            return df.to_dict('records')
            
        except Exception as e:
            print(f"Error processing data: {str(e)}")
            raise

    @task
    def save_to_csv(**context):
        """
        Save processed data to CSV with year-month folder structure
        """
        # Get processed data from previous task
        df_json = context['task_instance'].xcom_pull(task_ids='parse_and_process_data', key='processed_data')
        
        # Convert back to DataFrame
        df = pd.read_json(df_json, orient='records')
        
        # Create year-month folder structure
        current_date = datetime.datetime.now()
        year_month = current_date.strftime("%Y-%m")
        
        # Create directory structure
        base_dir = "include/datasets"
        year_month_dir = os.path.join(base_dir, year_month)
        
        # Create directories if they don't exist
        os.makedirs(year_month_dir, exist_ok=True)
        
        # Generate filename with timestamp
        timestamp = current_date.strftime("%Y%m%d_%H%M%S")
        filename = f"property_listing_job_{timestamp}.csv"
        filepath = os.path.join(year_month_dir, filename)
        
        try:
            # Save DataFrame to CSV
            df.to_csv(filepath, index=False, encoding='utf-8')
            
            print(f"Data saved successfully to: {filepath}")
            print(f"File size: {os.path.getsize(filepath)} bytes")
            print(f"Records saved: {len(df)}")
            
            # Store filepath and year_month in XCom for downstream tasks
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
        # Get filepath and year_month from previous task
        csv_filepath = context['task_instance'].xcom_pull(task_ids='save_to_csv', key='csv_filepath')
        year_month = context['task_instance'].xcom_pull(task_ids='save_to_csv', key='year_month')
        
        # Get GCS bucket from environment variable
        gcs_bucket = os.getenv("GCS_BUCKET_RAW", "niblr-raw-layer-dev")
        
        # Create GCS destination path
        gcs_destination = f"property_listing_raw/{year_month}/"
        
        print(f"Preparing to trigger upload DAG:")
        print(f"  Source file: {csv_filepath}")
        print(f"  GCS bucket: {gcs_bucket}")
        print(f"  GCS destination: {gcs_destination}")
        print(f"  Year-month: {year_month}")
        
        # Configuration for the upload DAG
        upload_config = {
            'source_file_path': csv_filepath,
            'gcs_bucket': gcs_bucket,
            'gcs_destination': gcs_destination,
            'mime_type': 'text/csv',
            'gzip': True,
            'bigquery_dataset_id': 'property_listing',
            'bigquery_table_name': 'property_listing_stg',
            'bigquery_schema_name': 'property_listing',
            'create_dataset_if_missing': True,
            'use_native_support': False,
            'if_exists': 'replace',
            'metadata': {
                'source': 'property_api',
                'data_type': 'property_listings',
                'upload_date': '{{ ds }}',
                'year_month': year_month,
                'environment': 'development'
            }
        }
        
        print(f"Upload configuration prepared:")
        for key, value in upload_config.items():
            print(f"  {key}: {value}")
        
        # Trigger the generic upload DAG directly
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
    api_data = fetch_api_data()
    processed_data = parse_and_process_data()
    csv_file = save_to_csv()
    upload_result = trigger_upload_to_gcs_and_bigquery()
    
    # Set up the workflow
    api_data >> processed_data >> csv_file >> upload_result

# Create the DAG instance
property_api_dag = property_api_pipeline()