"""
Generic Upload to GCS and BigQuery Job
This DAG provides a reusable service for uploading files to GCS and loading them to BigQuery.
"""

import datetime
import os
from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from typing import Dict, Any, Optional

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

@dag(
    dag_id="upload_file_gc_job",
    default_args=default_args,
    description="Generic upload service for files to GCS and BigQuery",
    schedule_interval=None,
    catchup=False,
    tags=['upload', 'gcs', 'bigquery', 'generic'],
    params={
        # File upload parameters
        'source_file_path': None,  # Local file path to upload
        'gcs_bucket': None,        # GCS bucket name
        'gcs_destination': None,   # GCS destination path/folder
        'gcs_object_name': None,   # Specific object name (optional)
        'mime_type': 'application/octet-stream',  # MIME type
        'gzip': False,             # Whether to gzip the file
        
        # BigQuery parameters
        'bigquery_dataset_id': None,  # BigQuery dataset ID
        'bigquery_table_name': None,  # BigQuery table name
        'bigquery_schema_name': None, # BigQuery schema name (optional, defaults to dataset_id)
        'create_dataset_if_missing': True,  # Create dataset if it doesn't exist
        'source_format': 'CSV',     # Source format: CSV, JSON, PARQUET, AVRO, ORC
        'if_exists': 'replace',     # What to do if table exists ('replace', 'append', 'fail')
        'write_disposition': 'WRITE_TRUNCATE',  # WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
        'autodetect': True,         # Auto-detect schema from data
        'skip_leading_rows': 1,     # Skip header rows for CSV
        'field_delimiter': ',',     # Field delimiter for CSV
        'allow_quoted_newlines': True,  # Allow quoted newlines in CSV
        'allow_jagged_rows': True,  # Allow jagged rows in CSV
        
        # Additional parameters
        'project_id': None,        # GCP project ID
        'gcp_conn_id': 'google_cloud_default',  # GCP connection ID
        'metadata': None,          # Additional metadata for GCS
    }
)
def upload_file_gc_job():
    """
    Generic upload DAG that uploads files to GCS and loads them to BigQuery
    """
    
    @task
    def validate_upload_parameters(**context):
        """
        Validate and prepare upload parameters
        """
        params = context['params']
        
        # Required parameters
        required_params = ['source_file_path', 'gcs_bucket', 'gcs_destination', 
                          'bigquery_dataset_id', 'bigquery_table_name']
        missing_params = [param for param in required_params if not params.get(param)]
        
        if missing_params:
            raise ValueError(f"Missing required parameters: {missing_params}")
        
        # Validate source file exists
        source_file = params['source_file_path']
        if not os.path.exists(source_file):
            raise FileNotFoundError(f"Source file not found: {source_file}")
        
        # Build GCS object path
        if params.get('gcs_object_name'):
            gcs_object = f"{params['gcs_destination'].rstrip('/')}/{params['gcs_object_name']}"
        else:
            # Use original filename
            filename = os.path.basename(source_file)
            gcs_object = f"{params['gcs_destination'].rstrip('/')}/{filename}"
        
        # Set default schema name if not provided
        schema_name = params.get('bigquery_schema_name', params['bigquery_dataset_id'])
        
        # Determine source format from file extension or MIME type
        source_format = params.get('source_format', 'CSV')
        if not source_format:
            file_ext = os.path.splitext(source_file)[1].lower()
            if file_ext == '.json':
                source_format = 'JSON'
            elif file_ext == '.parquet':
                source_format = 'PARQUET'
            elif file_ext == '.avro':
                source_format = 'AVRO'
            elif file_ext == '.orc':
                source_format = 'ORC'
            else:
                source_format = 'CSV'
        
        # Map if_exists to write_disposition
        if_exists = params.get('if_exists', 'replace')
        if if_exists == 'replace':
            write_disposition = 'WRITE_TRUNCATE'
        elif if_exists == 'append':
            write_disposition = 'WRITE_APPEND'
        else:  # 'fail'
            write_disposition = 'WRITE_EMPTY'
        
        # Prepare upload configuration
        upload_config = {
            'source_file': source_file,
            'gcs_bucket': params['gcs_bucket'],
            'gcs_object': gcs_object,
            'mime_type': params.get('mime_type', 'application/octet-stream'),
            'gzip': params.get('gzip', False),
            'bigquery_dataset_id': params['bigquery_dataset_id'],
            'bigquery_table_name': params['bigquery_table_name'],
            'bigquery_schema_name': schema_name,
            'create_dataset_if_missing': params.get('create_dataset_if_missing', True),
            'source_format': source_format,
            'write_disposition': write_disposition,
            'autodetect': params.get('autodetect', True),
            'skip_leading_rows': params.get('skip_leading_rows', 1),
            'field_delimiter': params.get('field_delimiter', ','),
            'allow_quoted_newlines': params.get('allow_quoted_newlines', True),
            'allow_jagged_rows': params.get('allow_jagged_rows', True),
            'project_id': params.get('project_id'),
            'gcp_conn_id': params.get('gcp_conn_id', 'google_cloud_default'),
            'metadata': params.get('metadata'),
        }
        
        print(f"Upload configuration validated:")
        print(f"  Source: {upload_config['source_file']}")
        print(f"  GCS Destination: gs://{upload_config['gcs_bucket']}/{upload_config['gcs_object']}")
        print(f"  BigQuery Dataset: {upload_config['bigquery_dataset_id']}")
        print(f"  BigQuery Table: {upload_config['bigquery_table_name']}")
        print(f"  Source Format: {upload_config['source_format']}")
        print(f"  Write Disposition: {upload_config['write_disposition']}")
        print(f"  Auto-detect Schema: {upload_config['autodetect']}")
        
        # Store configuration in XCom
        context['task_instance'].xcom_push(key='upload_config', value=upload_config)
        
        return upload_config
    
    @task
    def upload_to_gcs(**context):
        """
        Upload file to GCS using the validated configuration
        """
        upload_config = context['task_instance'].xcom_pull(task_ids='validate_upload_parameters', key='upload_config')
        
        print(f"Uploading {upload_config['source_file']} to GCS bucket {upload_config['gcs_bucket']} at {upload_config['gcs_object']}")
        
        # Create upload operator
        upload_task = LocalFilesystemToGCSOperator(
            task_id='upload_file_to_gcs',
            src=upload_config['source_file'],
            dst=upload_config['gcs_object'],
            bucket=upload_config['gcs_bucket'],
            gcp_conn_id=upload_config['gcp_conn_id'],
            mime_type=upload_config['mime_type'],
            gzip=upload_config['gzip'],
        )
        
        # Execute the upload
        upload_task.execute(context)
        
        # Build the full GCS path
        gcs_path = f"gs://{upload_config['gcs_bucket']}/{upload_config['gcs_object']}"
        
        print(f"✅ File uploaded successfully to: {gcs_path}")
        
        # Store result in XCom
        context['task_instance'].xcom_push(key='gcs_path', value=gcs_path)
        
        return gcs_path
    
    @task
    def create_bigquery_dataset(**context):
        """
        Create BigQuery dataset if it doesn't exist
        """
        upload_config = context['task_instance'].xcom_pull(task_ids='validate_upload_parameters', key='upload_config')
        
        if upload_config['create_dataset_if_missing']:
            print(f"Creating BigQuery dataset: {upload_config['bigquery_dataset_id']}")
            
            create_dataset_task = BigQueryCreateEmptyDatasetOperator(
                task_id='create_bigquery_dataset',
                dataset_id=upload_config['bigquery_dataset_id'],
                gcp_conn_id=upload_config['gcp_conn_id'],
                project_id=upload_config.get('project_id'),
            )
            
            create_dataset_task.execute(context)
            print(f"✅ BigQuery dataset created: {upload_config['bigquery_dataset_id']}")
        else:
            print(f"⏭️ Skipping dataset creation for: {upload_config['bigquery_dataset_id']}")
    
    @task
    def load_to_bigquery(**context):
        """
        Load file from GCS to BigQuery using native Airflow operator
        """
        upload_config = context['task_instance'].xcom_pull(task_ids='validate_upload_parameters', key='upload_config')
        gcs_path = context['task_instance'].xcom_pull(task_ids='upload_to_gcs', key='gcs_path')
        
        print(f"Loading data from {gcs_path} to BigQuery table: {upload_config['bigquery_table_name']}")
        
        # Build the full table name
        if upload_config.get('project_id'):
            table_name = f"{upload_config['project_id']}.{upload_config['bigquery_dataset_id']}.{upload_config['bigquery_table_name']}"
        else:
            table_name = f"{upload_config['bigquery_dataset_id']}.{upload_config['bigquery_table_name']}"
        
        # Create the load task using native BigQuery operator
        load_task = GCSToBigQueryOperator(
            task_id='load_to_bigquery',
            bucket=upload_config['gcs_bucket'],
            source_objects=[upload_config['gcs_object']],
            destination_project_dataset_table=table_name,
            source_format=upload_config['source_format'],
            write_disposition=upload_config['write_disposition'],
            autodetect=upload_config['autodetect'],
            skip_leading_rows=upload_config['skip_leading_rows'],
            field_delimiter=upload_config['field_delimiter'],
            allow_quoted_newlines=upload_config['allow_quoted_newlines'],
            allow_jagged_rows=upload_config['allow_jagged_rows'],
            gcp_conn_id=upload_config['gcp_conn_id'],
        )
        
        # Execute the load
        load_task.execute(context)
        
        print(f"✅ Data loaded successfully to BigQuery table: {table_name}")
        
        # Store result data
        load_result = {
            'table_name': table_name,
            'dataset_id': upload_config['bigquery_dataset_id'],
            'gcs_path': gcs_path,
            'source_format': upload_config['source_format'],
            'write_disposition': upload_config['write_disposition'],
            'status': 'success',
            'timestamp': datetime.datetime.now().isoformat()
        }
        
        context['task_instance'].xcom_push(key='bigquery_result', value=load_result)
        
        return load_result
    
    # Define task dependencies
    validation = validate_upload_parameters()
    upload = upload_to_gcs()
    create_dataset = create_bigquery_dataset()
    load = load_to_bigquery()
    
    # Set up the workflow
    validation >> upload >> create_dataset >> load

# Create the DAG instance
upload_file_gc_job_instance = upload_file_gc_job() 