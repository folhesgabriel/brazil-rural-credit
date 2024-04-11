
import pandas as pd
from io import StringIO
from tqdm.asyncio import tqdm
from tqdm import tqdm
import os
import aiohttp
from aiohttp import ClientSession
import asyncio
from pathlib import Path
import csv

from google.cloud import storage, bigquery
from typing import Any, Dict, List, Optional, Tuple, Union
from prefect_pipelines.brazil_bcb_sicor.constants import Constants
import asyncio
import aiohttp
import os
import csv
import io
import pandas as pd
from tqdm import tqdm
from aiohttp import ClientSession
from google.cloud import bigquery
from google.cloud import storage
from typing import List, Tuple, Dict, Any, Union
from pathlib import Path


async def download_file(session: ClientSession, url: str, folder: str, semaphore: asyncio.Semaphore) -> None:
    """
    Downloads a file from a URL and saves it to a specific directory.
    Uses a semaphore to limit the number of parallel downloads.
    
    Args:
        session (ClientSession): The aiohttp session to make the request.
        url (str): The URL of the file to be downloaded.
        folder (str): The directory where the file will be saved.
        semaphore (asyncio.Semaphore): The semaphore to control concurrency.
    """
    async with semaphore:
        filename = os.path.basename(url)
        filepath = os.path.join(folder, filename)
        
        async with session.get(url) as response:
            if response.status == 200:
                total_size = int(response.headers.get('content-length', 0))
                with open(filepath, 'wb') as f, tqdm(
                    desc=filename,
                    total=total_size,
                    unit='iB',
                    unit_scale=True,
                    unit_divisor=1024,
                ) as bar:
                    while True:
                        chunk = await response.content.read(1024)
                        if not chunk:
                            break
                        f.write(chunk)
                        bar.update(len(chunk))
                print(f"File {filename} downloaded successfully.")
            else:
                print(f"Error downloading file {filename}. Status: {response.status}")


async def download_files(urls: List[str], download_folder: str, table_id: str, max_parallel: int) -> None:
    """
    Downloads all files from the provided URLs asynchronously.
    Limits the number of parallel downloads with a semaphore.
    
    Args:
        urls (List[str]): The list of URLs of the files to be downloaded.
        download_folder (str): The directory where the files will be saved.
        max_parallel (int): The maximum number of parallel downloads.
    """
    semaphore = asyncio.Semaphore(max_parallel)
    table_id_path = os.path.join(download_folder, table_id)
    
    if not os.path.exists(table_id_path):
        os.makedirs(table_id_path)
    
    async with aiohttp.ClientSession() as session:
        tasks = [download_file(session, url, table_id_path, semaphore) for url in urls]
        await asyncio.gather(*tasks)


def pre_process_files(download_folder: str, table_id: str) -> None:
    """
    Pre-processes files by renaming columns and saving them as Parquet chunks.
    
    Args:
        download_folder (str): The directory where the files are downloaded.
        table_id (str): The table ID.
    """
    table_id_path = os.path.join(download_folder, table_id)
    os.makedirs(table_id_path, exist_ok=True)

    DTYPES: Dict[str, Any] = Constants.DTYPES.value[table_id]
    COL_NAMES: Dict[str, str] = Constants.COL_NAMES.value[table_id]

    file_list = [os.path.join(table_id_path, file) for file in os.listdir(table_id_path)]

    for file in file_list:
        base_filename = os.path.splitext(os.path.basename(file))[0]
        print(f"Processing file {base_filename}.")
        
        chunksize = 1024 * 1024  # Adjusted based on the average line size to approximate 30MB per chunk
        
        for i, chunk in enumerate(pd.read_csv(
            file, 
            sep=';', 
            encoding='latin1', 
            dtype=DTYPES,
            chunksize=chunksize
        )):
            # Rename columns in the chunk as necessary
            chunk = chunk.rename(columns=COL_NAMES)

            chunk_filename = f"{base_filename}_chunk_{i}.parquet"
            chunk.to_parquet(os.path.join(table_id_path, chunk_filename), index=False)
            
            print(f"Chunk {i} of file {base_filename} saved as {chunk_filename}.")


def run_dbt_model(model_name: str) -> None:
    """
    Runs a dbt model.
    
    Args:
        model_name (str): The name of the dbt model.
    """
    print(f"Running dbt model {model_name}")
    dbt_command = f"dbt run --select models/brazil_rural_credit/{model_name}"
    print(f"Running command: {dbt_command}")
    dbt_project_dir = "~/brazil-rural-credit/rural_credit"
    os.system(f"cd {dbt_project_dir} && {dbt_command}")


def create_dataset_and_table_with_inferred_schema(project_id: str, dataset_id: str, table_id: str, bucket_name: str) -> None:
    """
    Creates a dataset and table in BigQuery, infers the schema from a file in GCS, and loads all files data.

    Args:
        project_id (str): GCP project ID.
        dataset_id (str): BigQuery dataset ID.
        table_id (str): BigQuery table ID.
        bucket_name (str): Name of the GCS bucket.
    """
    source_folder_path = f"staging/{dataset_id}/{table_id}/"
    
    client = bigquery.Client(project=project_id)
    staging_dataset_id = f"{dataset_id}_staging"
    # List files in the specified directory
    blob_names = list_blobs_in_folder(bucket_name, source_folder_path)
    if not blob_names:
        raise ValueError(f"No files found in gs://{bucket_name}/{source_folder_path}.")

    # Choose the first file for schema inference
    blob_name = blob_names[0]
    print(f"Inferring schema from file: {blob_name}")

    # Infer schema
    schema = infer_schema_from_file(bucket_name, blob_name)

    # Create Dataset
    dataset_ref = bigquery.DatasetReference(project_id, staging_dataset_id)
    dataset = bigquery.Dataset(dataset_ref)
    dataset = client.create_dataset(dataset, exists_ok=True)
    print(f"Dataset {staging_dataset_id} created.")

    # Create Table with inferred schema
    table_ref = dataset_ref.table(table_id)
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        autodetect=False, 
        source_format=bigquery.SourceFormat.CSV if '.csv' in blob_name else bigquery.SourceFormat.PARQUET
    )

    # Generate URIs for all files
    source_uris = [f"gs://{bucket_name}/staging/{dataset_id}/{table_id}/{name.split('/')[-1]}" for name in blob_names]
    
    print(f'Loading {len(source_uris)} files from {source_folder_path} into {table_id}...')
    
    load_job = client.load_table_from_uri(source_uris, table_ref, job_config=job_config)

    # Wait for the load job to complete
    load_job.result()

    print(f"Table {table_id} created and data loaded with inferred schema.")


def infer_schema_from_file(bucket_name: str, blob_name: str, extra_columns: List[Tuple[str, str]] = []) -> List[bigquery.SchemaField]:
    """
    Infers the schema of a file (CSV or Parquet) stored in GCS.

    Args:
        bucket_name (str): Name of the GCS bucket.
        blob_name (str): Name of the blob (file) in the bucket.
        extra_columns (List[Tuple[str, str]]): Additional columns to include in the schema.

    Returns:
        A list of bigquery.SchemaField objects with field names and types.
    """
    # Determine the file extension
    _, file_extension = os.path.splitext(blob_name)
    file_extension = file_extension.lower()

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    contents = blob.download_as_bytes()

    if file_extension == '.csv':
        # Infer schema from CSV (assuming all fields are strings)
        try:
            reader = csv.reader(StringIO(contents.decode('utf-8')))
        except:
            reader = csv.reader(StringIO(contents.decode('latin-1'))) 
        headers = next(reader)
        schema = [bigquery.SchemaField(header, "STRING") for header in headers]

    elif file_extension == '.parquet':
        # Infer schema from Parquet (preserving data types)
        df = pd.read_parquet(io.BytesIO(contents), engine='pyarrow')
        schema = [bigquery.SchemaField(name, str(dtype), mode='NULLABLE') for name, dtype in zip(df.columns, df.dtypes)]

        # Convert pandas dtype to BigQuery data types
        type_mapping = {
            'int64': 'INT64',
            'float64': 'FLOAT',
            'bool': 'BOOL',
            'object': 'STRING',  # Assuming object dtype are strings; adjust as needed
            # Add more mappings as necessary
        }
        schema = [bigquery.SchemaField(field.name, type_mapping.get(str(field.field_type), 'STRING')) for field in schema]

    else:
        raise ValueError(f"Unsupported file extension: {file_extension}")

    for col_name, col_type in extra_columns:
        schema.append(bigquery.SchemaField(col_name, col_type))
        
    return schema


def build_blob_name(filename: str, mode: str, dataset_id: str, table_id: str, partitions: Union[Dict[str, str], str] = None) -> str:
    """
    Builds the blob name.

    Args:
        filename (str): The name of the file.
        mode (str): The mode of the blob (e.g., 'staging', 'production').
        dataset_id (str): The dataset ID.
        table_id (str): The table ID.
        partitions (Union[Dict[str, str], str]): The partitions of the blob.

    Returns:
        The blob name.
    """
    # Table folder
    blob_name = f"{mode}/{dataset_id}/{table_id}/"

    # Add partition folder
    if partitions is not None:
        blob_name += resolve_partitions(partitions)

    # Add file name
    blob_name += filename

    print(blob_name)
    return blob_name


def resolve_partitions(partitions: Union[Dict[str, str], str]) -> str:
    """
    Resolves the partitions into a string format.

    Args:
        partitions (Union[Dict[str, str], str]): The partitions.

    Returns:
        The string representation of the partitions.
    """
    if isinstance(partitions, dict):
        return "/".join(f"{k}={v}" for k, v in partitions.items()) + "/"

    if isinstance(partitions, str):
        if partitions.endswith("/"):
            partitions = partitions[:-1]

        # If there is no partition
        if len(partitions) == 0:
            return ""

        # It should fail if there is a folder that is not a partition
        try:
            # Check if it fits the rule
            {b.split("=")[0]: b.split("=")[1] for b in partitions.split("/")}
        except IndexError as e:
            raise Exception(f"The path {partitions} is not a valid partition") from e

        return partitions + "/"

    raise Exception(f"Partitions format or type not accepted: {partitions}")


def upload_to_gcs(bucket_name: str, dataset_id: str, table_id: str, download_folder: str) -> None:
    """
    Uploads files to the specified GCS bucket. Handles both individual files and directories, maintaining Hive partitioning format.

    Args:
        bucket_name (str): Name of the GCS bucket.
        dataset_id (str): Identifier for the dataset, used to create a folder in the bucket.
        table_id (str): Identifier for the table, used to create a folder within the dataset folder.
        download_folder (str): Path to the file or directory to upload.
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    table_id_path = os.path.join(download_folder, table_id)

    path = Path(table_id_path)

    if path.is_dir():
        # Extract paths
        paths = [
            f
            for f in path.glob("**/*")
            if f.is_file() and f.suffix in [".csv", ".parquet", "parquet.gzip"]
        ]

        # Extract path parts (partitions)
        parts = [
            (
                filepath.as_posix()
                .replace(path.as_posix() + "/", "")
                .replace(str(filepath.name), "")
            )
            for filepath in paths
        ]

    else:
        paths = [path]
        parts = [None]
    
    # TODO: IMPLEMENT CHUNKSIZE OPTION TO RESILIENT DOWNLOAD
        
    for filepath, part in tqdm(list(zip(paths, parts)), desc="Uploading files"):
        blob_name = build_blob_name(
            mode='staging', 
            filename=filepath.name, 
            table_id=table_id,   
            dataset_id=dataset_id, 
            partitions=part)
       
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(str(filepath))


def list_blobs_in_folder(bucket_name: str, folder_path: str) -> List[str]:
    """
    Lists all blobs in the specified bucket that are in the specified directory.
    
    Args:
        bucket_name (str): Name of the GCS bucket.
        folder_path (str): Path of the folder within the bucket. Make sure it ends with '/'.
    
    Returns:
        A list of blob names found in the folder.
    """
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name, prefix=folder_path)
    blob_names = [blob.name for blob in blobs if not blob.name.endswith('/')]

    return blob_names
