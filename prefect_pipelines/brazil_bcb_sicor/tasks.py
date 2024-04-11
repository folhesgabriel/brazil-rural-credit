from prefect import task
import asyncio
from prefect_pipelines.brazil_bcb_sicor.constants import Constants as bcb_contants
from typing import Any, Dict, List, Optional, Tuple, Union

from prefect_pipelines.brazil_bcb_sicor.utils import (
    upload_to_gcs, 
    create_dataset_and_table_with_inferred_schema,
    download_files,
    pre_process_files,
    run_dbt_model,
    )



@task
def download_files_task(urls: List[str], download_folder: str, table_id: str, max_parallel: int = 5):
    """
    Downloads files from the given URLs to the specified download folder.
    
    Args:
        urls (List[str]): List of URLs to download files from.
        download_folder (str): Path to the folder where the files will be downloaded.
        table_id (str): Identifier for the table associated with the downloaded files.
        max_parallel (int, optional): Maximum number of parallel downloads. Defaults to 5.
    
    Returns:
        None
        
    """
    #TODO:
    #https://stackoverflow.com/questions/58571343/downloading-a-large-file-in-parts-using-multiple-parallel-threads
    
    return asyncio.run(download_files(urls=urls, download_folder=download_folder, max_parallel=max_parallel, table_id=table_id))


@task
def pre_process_files_task(download_folder: str, table_id: str,pre_process_files_folder:str):
    """
    Pre-processes the files in the specified download folder.
    
    Args:
        download_folder (str): Path to the folder containing the downloaded files.
        table_id (str): Identifier for the table associated with the pre-processed files.
    
    Returns:
        None
    """
    return pre_process_files(download_folder=download_folder, pre_process_files_folder=pre_process_files_folder,table_id=table_id)


@task
def upload_to_gcs_task(bucket_name: str, dataset_id: str, table_id: str, pre_process_files_folder: str):
    """
    Uploads the files from the specified download folder to Google Cloud Storage.
    
    Args:
        bucket_name (str): Name of the Google Cloud Storage bucket.
        dataset_id (str): Identifier for the dataset in BigQuery.
        table_id (str): Identifier for the table in BigQuery.
        download_folder (str): Path to the folder containing the files to upload.
    
    Returns:
        None
    """
    return upload_to_gcs(
        bucket_name=bucket_name,
        dataset_id=dataset_id,
        table_id=table_id,
        pre_process_files_folder=pre_process_files_folder,
    )


@task
def create_dataset_and_table_with_inferred_schema_task(project_id: str, dataset_id: str, table_id: str, bucket_name: str):
    """
    Creates a dataset and table in BigQuery with inferred schema based on the uploaded files.
    
    Args:
        project_id (str): Identifier for the Google Cloud project.
        dataset_id (str): Identifier for the dataset in BigQuery.
        table_id (str): Identifier for the table in BigQuery.
        bucket_name (str): Name of the Google Cloud Storage bucket.
    
    Returns:
        None
    """
    return create_dataset_and_table_with_inferred_schema(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name=bucket_name,
    )


@task
def run_dbt_model_task(model_name: str):
    """
    Triggers the execution of a dbt model.
    
    Args:
        model_name (str): Name of the dbt model to run.
    
    Returns:
        None
    """
    return run_dbt_model(
        model_name=model_name
    )
