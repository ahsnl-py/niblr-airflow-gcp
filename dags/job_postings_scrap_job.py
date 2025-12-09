"""
Job Postings Scrap Job Pipeline
This DAG queries BigQuery to get URLs from job_listings_partitioned table,
then scrapes detailed job posting data from each URL and uploads to GCS/BigQuery.
"""

import datetime
import os
import json
import pandas as pd
import time
import requests
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from typing import Dict, Any, Optional, List
from google.cloud import bigquery
from google.cloud.exceptions import NotFound


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
    dag_id="job_postings_scrap_job",
    default_args=default_args,
    description="Scrape detailed job postings from URLs queried from BigQuery",
    schedule_interval=None,
    catchup=False,
    tags=['scraping', 'api', 'job-postings', 'data-pipeline'],
    params={
        'config_file': 'include/configs/job_postings.json',  # Config file path
        'limit': None,  # Optional: Limit number of URLs to process
        'where_clause': None,  # Optional: Additional WHERE clause for query
    }
)
def job_postings_scrap_job():

    @task
    def load_job_config(**context):
        """
        Load job configuration from file
        """
        params = context['params']
        config_file = params.get('config_file', 'include/configs/job_postings.json')
        
        print(f"Loading configuration from: {config_file}")
        
        try:
            with open(config_file, 'r') as f:
                job_config = json.load(f)
            
            # Validate required configuration fields
            required_fields = ['api_base_url', 'source_query', 'job_payload', 'output_config']
            missing_fields = [field for field in required_fields if field not in job_config]
            
            if missing_fields:
                raise ValueError(f"Missing required configuration fields: {missing_fields}")
            
            print(f"✅ Configuration loaded successfully")
            print(f"Source query: {job_config['source_query']}")
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
    def query_bigquery_for_urls(**context):
        """
        Query BigQuery to fetch URLs from job_listings_partitioned table
        """
        job_config = context['task_instance'].xcom_pull(task_ids='load_job_config', key='job_config')
        params = context['params']
        
        source_query = job_config['source_query']
        project_id = source_query.get('project_id', 'niblr-agentic-service')
        dataset = source_query['dataset']
        table = source_query['table']
        url_column = source_query.get('url_column', 'link')
        job_id_column = source_query.get('job_id_column', 'job_id')
        
        # Build query
        limit_clause = ""
        if params.get('limit'):
            limit_clause = f"LIMIT {params['limit']}"
        
        where_clause = params.get('where_clause', '') or ''
        additional_where = f"AND {where_clause}" if where_clause.strip() else ""
        
        query = f"""
        SELECT 
            {job_id_column},
            {url_column}
        FROM `{project_id}.{dataset}.{table}`
        WHERE is_process = FALSE OR is_process IS NULL {additional_where}
        ORDER BY processed_at
        {limit_clause}
        """
        
        print(f"Executing BigQuery query:")
        print(query)
        
        try:
            # Initialize BigQuery client
            client = bigquery.Client(project=project_id)
            
            # Execute query
            query_job = client.query(query)
            results = query_job.result()
            
            # Extract URLs
            urls_data = []
            for row in results:
                urls_data.append({
                    'job_id': getattr(row, job_id_column, None),
                    'url': getattr(row, url_column, None)
                })
            
            print(f"✅ Fetched {len(urls_data)} URLs from BigQuery")
            
            # Store URLs in XCom
            context['task_instance'].xcom_push(key='urls_data', value=urls_data)
            
            return urls_data
            
        except NotFound as e:
            raise Exception(f"BigQuery table not found: {project_id}.{dataset}.{table}")
        except Exception as e:
            print(f"Error querying BigQuery: {str(e)}")
            raise

    @task
    def fetch_api_data_for_urls(**context):
        """
        Fetch data from API for all URLs in a single batch job
        """
        job_config = context['task_instance'].xcom_pull(task_ids='load_job_config', key='job_config')
        urls_data = context['task_instance'].xcom_pull(task_ids='query_bigquery_for_urls', key='urls_data')
        
        api_base_url = job_config['api_base_url']
        polling = job_config.get('polling', {})
        max_attempts = polling.get('max_attempts', 30)
        poll_delay = polling.get('base_delay', 30)
        
        # Extract URLs and create mapping
        urls = [entry['url'] for entry in urls_data if entry.get('url')]
        url_to_job_id = {entry['url']: entry.get('job_id') for entry in urls_data if entry.get('url')}
        
        if not urls:
            raise ValueError("No valid URLs found to process")
        
        print(f"Processing {len(urls)} URLs in batch job")
        
        # Build job payload
        job_payload = {**job_config['job_payload']}
        job_payload.setdefault('metadata', {})['urls'] = urls
        job_payload['metadata'].setdefault('execution_mode', 'sequential')
        
        # Step 1: Create job
        print("Creating job...")
        try:
            response = requests.post(f"{api_base_url}/jobs", json=job_payload)
            response.raise_for_status()
            job_data = response.json()
            api_job_id = job_data['id']
            print(f"Job ID: {api_job_id}, Status: {job_data.get('status')}")
        except requests.exceptions.RequestException as e:
            raise Exception(f"Failed to create job: {str(e)}")
        
        # Step 2: Poll until completed
        print("Waiting for job to complete...")
        attempt = 0
        
        while attempt < max_attempts:
            attempt += 1
            try:
                response = requests.get(f"{api_base_url}/jobs/{api_job_id}")
                response.raise_for_status()
                job_data = response.json()
                status = job_data.get('status')
                print(f"Status: {status}")
                
                if status == 'completed':
                    break
                elif status == 'failed':
                    error = job_data.get('error_message', 'Unknown error')
                    raise Exception(f"Job failed: {error}")
                elif status in ['pending', 'running']:
                    time.sleep(poll_delay)
                else:
                    raise Exception(f"Unknown status: {status}")
                    
            except requests.exceptions.RequestException as e:
                if attempt >= max_attempts:
                    raise Exception(f"Failed to check status after {max_attempts} attempts: {str(e)}")
                print(f"Request error (attempt {attempt}): {e}, retrying...")
                time.sleep(poll_delay)
        
        # Cancel job if max attempts reached
        if attempt >= max_attempts:
            print(f"Max attempts ({max_attempts}) reached. Cancelling job...")
            try:
                requests.delete(f"{api_base_url}/jobs/{api_job_id}")
                print(f"Job cancelled")
            except requests.exceptions.RequestException as e:
                print(f"Warning: Failed to cancel job: {str(e)}")
            raise Exception(f"Job did not complete after {max_attempts} attempts")
        
        # Step 3: Get result
        print("Fetching result...")
        try:
            response = requests.get(f"{api_base_url}/jobs/{api_job_id}/result")
            response.raise_for_status()
            result = response.json()
            extracted_data = result.get('data', [])
        except requests.exceptions.RequestException as e:
            raise Exception(f"Failed to fetch results: {str(e)}")
        
        # Enrich with job_id from source
        for item in extracted_data:
            if isinstance(item, dict):
                source_url = item.get('source_url') or item.get('url')
                if source_url in url_to_job_id:
                    item['job_id'] = url_to_job_id[source_url]
        
        print(f"✅ Completed: {len(extracted_data)} items extracted")
        
        # Store processed job_ids for flagging in BigQuery
        processed_job_ids = [job_id for job_id in url_to_job_id.values() if job_id is not None]
        context['task_instance'].xcom_push(key='api_data', value=extracted_data)
        context['task_instance'].xcom_push(key='processed_job_ids', value=processed_job_ids)
        context['task_instance'].xcom_push(key='failed_urls', value=[])
        
        return extracted_data

    @task
    def save_to_json(**context):
        """
        Save API data to JSON file with processing (better for ARRAY fields)
        """
        job_config = context['task_instance'].xcom_pull(task_ids='load_job_config', key='job_config')
        api_data = context['task_instance'].xcom_pull(task_ids='fetch_api_data_for_urls', key='api_data')
        
        output_config = job_config['output_config']
        
        try:
            if not api_data:
                print("⚠️ No data to save")
                return None
            
            # Filter out items with error=True
            filtered_data = [
                item for item in api_data 
                if not (isinstance(item, dict) and item.get('error') == True)
            ]
            
            # Add timestamp to each record
            current_timestamp = datetime.datetime.now().isoformat()
            for item in filtered_data:
                if isinstance(item, dict):
                    item['processed_at'] = current_timestamp
            
            print(f"Processing {len(filtered_data)} records")
            if filtered_data:
                print(f"Sample record keys: {list(filtered_data[0].keys()) if isinstance(filtered_data[0], dict) else 'N/A'}")
            
            # Create folder structure
            current_date = datetime.datetime.now()
            year_month = current_date.strftime("%Y-%m")
            
            # Create directory structure
            base_dir = "include/datasets"
            year_month_dir = os.path.join(base_dir, year_month)
            os.makedirs(year_month_dir, exist_ok=True)
            
            # Generate filename
            timestamp = current_date.strftime("%Y%m%d_%H%M%S")
            filename = f"job_postings_{timestamp}.json"
            filepath = os.path.join(year_month_dir, filename)
            
            # Save data as NEWLINE_DELIMITED_JSON format for BigQuery
            # Format: One JSON object per line, no array wrapper, no commas between objects
            print(f"Saving as NEWLINE_DELIMITED_JSON format (one JSON object per line)...")
            with open(filepath, 'w', encoding='utf-8') as f:
                for idx, item in enumerate(filtered_data, 1):
                    # Write each JSON object on its own line
                    json.dump(item, f, ensure_ascii=False)
                    f.write('\n')  # Newline after each JSON object
            
            # Verify the file format is correct
            print(f"Verifying NEWLINE_DELIMITED_JSON format...")
            with open(filepath, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                valid_lines = 0
                for line_num, line in enumerate(lines, 1):
                    line = line.strip()
                    if line:  # Skip empty lines
                        try:
                            json.loads(line)  # Verify each line is valid JSON
                            valid_lines += 1
                        except json.JSONDecodeError as e:
                            raise ValueError(f"Invalid JSON on line {line_num}: {str(e)}")
            
            print(f"✅ Data saved successfully to: {filepath}")
            print(f"  Format: NEWLINE_DELIMITED_JSON")
            print(f"  Valid JSON lines: {valid_lines}")
            print(f"  File size: {os.path.getsize(filepath)} bytes")
            print(f"  Records saved: {len(filtered_data)}")
            
            # Store filepath and metadata in XCom
            context['task_instance'].xcom_push(key='json_filepath', value=filepath)
            context['task_instance'].xcom_push(key='year_month', value=year_month)
            
            return filepath
            
        except Exception as e:
            print(f"Error saving JSON file: {str(e)}")
            raise

    @task
    def trigger_upload_to_gcs_and_bigquery(**context):
        """
        Trigger the generic upload DAG to upload JSON file to GCS and load to BigQuery
        """
        job_config = context['task_instance'].xcom_pull(task_ids='load_job_config', key='job_config')
        json_filepath = context['task_instance'].xcom_pull(task_ids='save_to_json', key='json_filepath')
        year_month = context['task_instance'].xcom_pull(task_ids='save_to_json', key='year_month')
        
        if not json_filepath:
            print("⚠️ No JSON file to upload")
            return None
        
        output_config = job_config['output_config']
        
        # Build GCS destination path
        gcs_destination = f"job_postings_stg_raw/{year_month}/"
        
        # Configuration for the upload DAG
        upload_config = {
            'source_file_path': json_filepath,
            'gcs_bucket': output_config['gcs_bucket'],
            'gcs_destination': gcs_destination,
            'mime_type': 'application/json',
            'gzip': True,
            'bigquery_dataset_id': output_config['bigquery_dataset'],
            'bigquery_table_name': output_config['bigquery_table'],
            'bigquery_schema_name': output_config.get('bigquery_schema', output_config['bigquery_dataset']),
            'create_dataset_if_missing': True,
            'source_format': 'NEWLINE_DELIMITED_JSON',
            'if_exists': output_config.get('table_action', 'append'),
            'autodetect': True,
            'skip_leading_rows': 0,  # JSON doesn't have header rows
            'metadata': {
                'source': 'api',
                'job_type': 'job_postings',
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

    @task
    def flag_processed_urls(**context):
        """
        Update BigQuery table to flag processed job_ids as is_process = True
        """
        job_config = context['task_instance'].xcom_pull(task_ids='load_job_config', key='job_config')
        processed_job_ids = context['task_instance'].xcom_pull(task_ids='fetch_api_data_for_urls', key='processed_job_ids')
        
        if not processed_job_ids:
            print("⚠️ No job_ids to flag as processed")
            return
        
        source_query = job_config['source_query']
        project_id = source_query.get('project_id', 'niblr-agentic-service')
        dataset = source_query['dataset']
        table = source_query['table']
        job_id_column = source_query.get('job_id_column', 'job_id')
        
        # Build UPDATE query using parameterized query for safety
        client = bigquery.Client(project=project_id)
        
        # Use parameterized query to safely handle job_ids
        # Convert job_ids to integers if they're strings, or keep as int if already int
        query_params = []
        placeholders = []
        for idx, job_id in enumerate(processed_job_ids):
            param_name = f"job_id_{idx}"
            # Convert to int if it's a string, otherwise use as-is
            job_id_value = int(job_id) if isinstance(job_id, str) else job_id
            query_params.append(bigquery.ScalarQueryParameter(param_name, "INT64", job_id_value))
            placeholders.append(f"@{param_name}")
        
        update_query = f"""
        UPDATE `{project_id}.{dataset}.{table}`
        SET is_process = TRUE
        WHERE {job_id_column} IN ({', '.join(placeholders)})
        """
        
        query_job_config = bigquery.QueryJobConfig(query_parameters=query_params)
        
        print(f"Updating {len(processed_job_ids)} job_ids as processed...")
        
        try:
            query_job = client.query(update_query, job_config=query_job_config)
            query_job.result()  # Wait for completion
            
            print(f"✅ Successfully flagged {len(processed_job_ids)} job_ids as processed")
            
            return len(processed_job_ids)
            
        except Exception as e:
            print(f"❌ Error flagging processed job_ids: {str(e)}")
            raise

    @task
    def merge_job_postings(**context):
        """
        Call BigQuery stored procedure to merge job postings
        """
        job_config = context['task_instance'].xcom_pull(task_ids='load_job_config', key='job_config')
        source_query = job_config['source_query']
        project_id = source_query.get('project_id', 'niblr-agentic-service')
        
        # Initialize BigQuery client
        client = bigquery.Client(project=project_id)
        
        # Call the stored procedure
        procedure_name = f"{project_id}.raw_layer.merge_job_postings"
        call_query = f"CALL `{procedure_name}`()"
        
        print(f"Calling BigQuery stored procedure: {procedure_name}")
        print(f"Query: {call_query}")
        
        try:
            query_job = client.query(call_query)
            query_job.result()  # Wait for completion
            
            print(f"✅ Successfully executed stored procedure: {procedure_name}")
            
            return True
            
        except Exception as e:
            print(f"❌ Error calling stored procedure: {str(e)}")
            raise

    # Define task dependencies
    config = load_job_config()
    urls = query_bigquery_for_urls()
    api_data = fetch_api_data_for_urls()
    json_file = save_to_json()
    upload_result = trigger_upload_to_gcs_and_bigquery()
    flag_urls = flag_processed_urls()
    merge_procedure = merge_job_postings()
    
    # Set up the workflow
    config >> urls >> api_data >> json_file >> upload_result >> flag_urls >> merge_procedure

# Create the DAG instance
job_postings_scrap_job_instance = job_postings_scrap_job()
