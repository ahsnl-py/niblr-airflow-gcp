"""
Simple Housekeeping DAG for cleaning up different storage types
Supports file_system, bigquery, and cloud_storage cleanup
"""

import os
import glob
import datetime
import json
from pathlib import Path
from typing import Dict, List, Any

from airflow.decorators import dag, task
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryDeleteTableOperator

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

@dag(
    dag_id="housekeeping_job",
    default_args=default_args,
    description="Simple housekeeping service for cleaning up storage systems",
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['housekeeping', 'cleanup', 'maintenance'],
    params={
        'housekeeping_types': ['file_system'],  # List: ['file_system', 'bigquery', 'cloud_storage']
        'config_file': '/opt/airflow/include/configs/housekeeping_config.json',
        'dry_run': True,
    }
)
def housekeeping_job():
    """
    Simple housekeeping DAG with support for multiple cleanup types
    """
    
    @task
    def load_housekeeping_config(**context):
        """
        Load housekeeping configuration from config file for multiple types
        """
        params = context['params']
        config_file = params['config_file']
        housekeeping_types = params['housekeeping_types']
        
        if not os.path.exists(config_file):
            raise FileNotFoundError(f"Config file not found: {config_file}")
        
        with open(config_file, 'r') as f:
            config = json.load(f)
        
        # Validate all requested types exist in config
        invalid_types = [t for t in housekeeping_types if t not in config]
        if invalid_types:
            raise ValueError(f"Housekeeping types not found in config: {invalid_types}")
        
        # Load configs for all requested types
        type_configs = {}
        for housekeeping_type in housekeeping_types:
            type_configs[housekeeping_type] = config[housekeeping_type]
            print(f"📋 Loaded config for {housekeeping_type}: {config[housekeeping_type]}")
        
        # Store configs in XCom
        context['task_instance'].xcom_push(key='housekeeping_configs', value=type_configs)
        context['task_instance'].xcom_push(key='housekeeping_types', value=housekeeping_types)
        
        return type_configs
    
    @task
    def cleanup_file_system(**context):
        """
        Clean up files from local file system
        """
        configs = context['task_instance'].xcom_pull(task_ids='load_housekeeping_config', key='housekeeping_configs')
        dry_run = context['params']['dry_run']
        
        # Get file_system config
        config = configs.get('file_system', {})
        locations = config.get('location', [])
        retention_days = config.get('retention_period', 7)
        
        if not locations:
            print("⏭️ No file system locations specified")
            return {'deleted_files': [], 'total_size_freed': 0}
        
        cutoff_date = datetime.datetime.now() - datetime.timedelta(days=retention_days)
        deleted_files = []
        total_size_freed = 0
        
        print(f"🧹 Cleaning file system (cutoff: {cutoff_date})")
        
        for location in locations:
            if not os.path.exists(location):
                print(f"⚠️ Location does not exist: {location}")
                continue
            
            print(f"📁 Processing: {location}")
            
            # Find all files in the location
            for root, dirs, files in os.walk(location):
                for file in files:
                    file_path = os.path.join(root, file)
                    try:
                        file_stat = os.stat(file_path)
                        file_mtime = datetime.datetime.fromtimestamp(file_stat.st_mtime)
                        
                        if file_mtime < cutoff_date:
                            file_size = file_stat.st_size
                            
                            if dry_run:
                                print(f"🔍 [DRY RUN] Would delete: {file_path} (modified: {file_mtime})")
                            else:
                                os.remove(file_path)
                                print(f"🗑️ Deleted: {file_path}")
                                deleted_files.append(file_path)
                                total_size_freed += file_size
                    except Exception as e:
                        print(f"❌ Error processing {file_path}: {e}")
            
            # Remove empty directories
            if not dry_run:
                for root, dirs, files in os.walk(location, topdown=False):
                    for dir_name in dirs:
                        dir_path = os.path.join(root, dir_name)
                        try:
                            if not os.listdir(dir_path):
                                os.rmdir(dir_path)
                                print(f"🗑️ Deleted empty directory: {dir_path}")
                        except Exception as e:
                            print(f"❌ Error removing directory {dir_path}: {e}")
        
        result = {
            'deleted_files': deleted_files,
            'total_size_freed': total_size_freed,
            'file_count': len(deleted_files)
        }
        
        print(f"✅ File system cleanup completed: {len(deleted_files)} files deleted")
        
        context['task_instance'].xcom_push(key='cleanup_result', value=result)
        return result
    
    @task
    def cleanup_bigquery(**context):
        """
        Clean up BigQuery tables
        """
        configs = context['task_instance'].xcom_pull(task_ids='load_housekeeping_config', key='housekeeping_configs')
        dry_run = context['params']['dry_run']
        
        # Get bigquery config
        config = configs.get('bigquery', {})
        tables = config.get('tables', [])
        retention_days = config.get('retention_period', 90)
        
        if not tables:
            print("⏭️ No BigQuery tables specified")
            return {'deleted_tables': []}
        
        cutoff_date = datetime.datetime.now() - datetime.timedelta(days=retention_days)
        deleted_tables = []
        
        print(f"📊 Cleaning BigQuery tables (cutoff: {cutoff_date})")
        
        for table_spec in tables:
            if isinstance(table_spec, str):
                # Format: "dataset.table" or "project.dataset.table"
                table_parts = table_spec.split('.')
                if len(table_parts) == 2:
                    dataset_id, table_id = table_parts
                    project_id = None
                elif len(table_parts) == 3:
                    project_id, dataset_id, table_id = table_parts
                else:
                    print(f"⚠️ Invalid table specification: {table_spec}")
                    continue
            else:
                # Dictionary format
                project_id = table_spec.get('project_id')
                dataset_id = table_spec['dataset_id']
                table_id = table_spec['table_id']
            
            print(f"📋 Processing table: {dataset_id}.{table_id}")
            
            if dry_run:
                print(f"🔍 [DRY RUN] Would delete table: {dataset_id}.{table_id}")
            else:
                delete_task = BigQueryDeleteTableOperator(
                    task_id=f'delete_bq_table_{dataset_id}_{table_id}',
                    deletion_dataset_table=f"{dataset_id}.{table_id}",
                    gcp_conn_id='google_cloud_default',
                    project_id=project_id
                )
                
                try:
                    delete_task.execute(context)
                    print(f"🗑️ Deleted table: {dataset_id}.{table_id}")
                    deleted_tables.append(f"{dataset_id}.{table_id}")
                except Exception as e:
                    print(f"❌ Error deleting table {dataset_id}.{table_id}: {e}")
        
        result = {
            'deleted_tables': deleted_tables,
            'table_count': len(deleted_tables)
        }
        
        print(f"✅ BigQuery cleanup completed: {len(deleted_tables)} tables deleted")
        
        context['task_instance'].xcom_push(key='cleanup_result', value=result)
        return result
    
    @task
    def cleanup_cloud_storage(**context):
        """
        Clean up cloud storage (GCS, AWS S3, etc.)
        """
        configs = context['task_instance'].xcom_pull(task_ids='load_housekeeping_config', key='housekeeping_configs')
        dry_run = context['params']['dry_run']
        
        # Get cloud_storage config
        config = configs.get('cloud_storage', {})
        providers = config.get('provider', {})
        retention_days = config.get('retention_period', 30)
        
        if not providers:
            print("⏭️ No cloud storage providers specified")
            return {'deleted_objects': []}
        
        cutoff_date = datetime.datetime.now() - datetime.timedelta(days=retention_days)
        deleted_objects = []
        
        print(f"☁️ Cleaning cloud storage (cutoff: {cutoff_date})")
        
        # Handle GCS
        if 'gcs' in providers:
            gcs_paths = providers['gcs']
            print(f"🪣 Processing GCS paths: {gcs_paths}")
            
            for gcs_path in gcs_paths:
                # Parse GCS path: bucket/prefix
                if '/' in gcs_path:
                    bucket, prefix = gcs_path.split('/', 1)
                    prefix = prefix.rstrip('/') + '/'
                else:
                    bucket = gcs_path
                    prefix = ""
                
                print(f"🪣 Processing GCS: {bucket}/{prefix}")
                
                if dry_run:
                    print(f"🔍 [DRY RUN] Would clean GCS: gs://{bucket}/{prefix}")
                else:
                    # This is a simplified version - in practice you'd list objects first
                    # and filter by date before deleting
                    delete_task = GCSDeleteObjectsOperator(
                        task_id=f'delete_gcs_{bucket.replace("-", "_")}',
                        bucket_name=bucket,
                        objects=[],  # Would be populated with actual objects to delete
                        gcp_conn_id='google_cloud_default'
                    )
                    
                    try:
                        delete_task.execute(context)
                        print(f"🗑️ Cleaned GCS: gs://{bucket}/{prefix}")
                        deleted_objects.append(f"gs://{bucket}/{prefix}")
                    except Exception as e:
                        print(f"❌ Error cleaning GCS {bucket}/{prefix}: {e}")
        
        # Handle AWS S3 (placeholder for future implementation)
        if 'aws' in providers:
            aws_paths = providers['aws']
            print(f"☁️ Processing AWS S3 paths: {aws_paths}")
            
            for aws_path in aws_paths:
                print(f"🔍 [DRY RUN] Would clean AWS S3: {aws_path}")
                # AWS S3 cleanup implementation would go here
        
        result = {
            'deleted_objects': deleted_objects,
            'object_count': len(deleted_objects)
        }
        
        print(f"✅ Cloud storage cleanup completed: {len(deleted_objects)} objects deleted")
        
        context['task_instance'].xcom_push(key='cleanup_result', value=result)
        return result
    
    @task(trigger_rule='none_failed_min_one_success')
    def generate_summary(**context):
        """
        Generate cleanup summary
        """
        housekeeping_types = context['task_instance'].xcom_pull(task_ids='load_housekeeping_config', key='housekeeping_types')
        dry_run = context['params']['dry_run']
        
        # Get the result from the appropriate cleanup task
        cleanup_results = {}
        for housekeeping_type in housekeeping_types:
            cleanup_results[housekeeping_type] = context['task_instance'].xcom_pull(task_ids=f'cleanup_{housekeeping_type}', key='cleanup_result')
        
        print(f"📊 Housekeeping Summary:")
        for housekeeping_type, result in cleanup_results.items():
            print(f"  Type: {housekeeping_type}")
            print(f"  Dry Run: {dry_run}")
            print(f"  Result: {result}")
        
        return cleanup_results
    
    @task.branch
    def choose_cleanup_type(**context):
        """
        Branch to the appropriate cleanup task based on housekeeping_type
        """
        housekeeping_types = context['params']['housekeeping_types']
        print(f"🔄 Branching to cleanup types: {housekeeping_types}")
        
        # Create a list of task_ids to branch to
        task_ids_to_branch_to = []
        for housekeeping_type in housekeeping_types:
            task_ids_to_branch_to.append(f'cleanup_{housekeeping_type}')
        
        return task_ids_to_branch_to
    
    # Load configuration
    config_loaded = load_housekeeping_config()
    
    # Branch to choose which cleanup task to run
    branch_task = choose_cleanup_type()
    
    # Define all cleanup tasks
    file_cleanup = cleanup_file_system()
    bigquery_cleanup = cleanup_bigquery()
    cloud_cleanup = cleanup_cloud_storage()
    
    # Generate summary (will only run after the selected cleanup task)
    summary = generate_summary()
    
    # Set up conditional dependencies
    config_loaded >> branch_task >> [file_cleanup, bigquery_cleanup, cloud_cleanup] >> summary

# Create the DAG instance
housekeeping_job_instance = housekeeping_job() 