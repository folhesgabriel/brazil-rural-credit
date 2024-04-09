
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




async def download_file(session: ClientSession, url: str, folder: str, semaphore: asyncio.Semaphore):
    """
    Baixa um arquivo de uma URL e o salva em um diretório específico.
    Usa um semáforo para limitar o número de downloads paralelos.
    
    Args:
        session (ClientSession): A sessão aiohttp para fazer o request.
        url (str): A URL do arquivo para ser baixado.
        folder (str): O diretório onde o arquivo será salvo.
        semaphore (asyncio.Semaphore): O semáforo para controlar a concorrência.
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
                print(f"Arquivo {filename} baixado com sucesso.")
            else:
                print(f"Erro ao baixar o arquivo {filename}. Status: {response.status}")


async def download_files(urls: list, folder: str, max_parallel: int):
    """
    Baixa todos os arquivos das URLs fornecidas de forma assíncrona.
    Limita o número de downloads paralelos com um semáforo.
    
    Args:
        urls (list): A lista de URLs dos arquivos a serem baixados.
        folder (str): O diretório onde os arquivos serão salvos.
        max_parallel (int): O número máximo de downloads paralelos.
    """
    semaphore = asyncio.Semaphore(max_parallel)
    
    if not os.path.exists(folder):
        os.makedirs(folder)
    
    async with aiohttp.ClientSession() as session:
        tasks = [download_file(session, url, folder, semaphore) for url in urls]
        await asyncio.gather(*tasks)


path = '/home/gabri/brazil-rural-credit/tmp/input'
output_path = '/home/gabri/brazil-rural-credit/tmp/output'


def pre_process_files(input_path, output_path, table_id):
    
    os.makedirs(output_path, exist_ok=True)

    
    DTYPES = Constants.DTYPES.value[table_id]
    COL_NAMES = Constants.COL_NAMES.value[table_id]

    file_list = [os.path.join(input_path, file) for file in os.listdir(input_path)]

    for file in file_list:
        base_filename = os.path.splitext(os.path.basename(file))[0]
        print(f"Processando arquivo {base_filename}.")
        
        chunksize = 1024*1024  # Ajuste baseado no tamanho médio de linha para aproximar 30MB por chunk
        
        for i, chunk in enumerate(pd.read_csv(
            file, 
            sep=';', 
            encoding='latin1', 
            dtype=DTYPES,
            chunksize=chunksize
        )):
            # Renomeia as colunas do chunk conforme necessário
            chunk = chunk.rename(columns=COL_NAMES)

            
            chunk_filename = f"{base_filename}_chunk_{i}.parquet"
            chunk.to_parquet(os.path.join(output_path, chunk_filename), index=False)
            
            print(f"Chunk {i} do arquivo {base_filename} salvo como {chunk_filename}.")


def run_dbt_model(model_name)-> None:
    """
    Run a dbt model.
    """
    print(f"Running dbt model {model_name}")
    dbt_command = f"dbt run --select models/brazil_rural_credit/{model_name}"
    print(f"Running command: {dbt_command}")
    dbt_project_dir = "~/brazil-rural-credit/rural_credit"
    os.system(f"cd {dbt_project_dir} && {dbt_command}")

#-- Functions addapted from https://github.com/basedosdados/mais/blob/v2.0.0/python-package/basedosdados/upload/storage.py
def build_blob_name(filename, mode,dataset_id,table_id, partitions=None):
        """
        Builds the blob name.
        """

        # table folder
        blob_name = f"{mode}/{dataset_id}/{table_id}/"

        # add partition folder
        if partitions is not None:
            blob_name += resolve_partitions(partitions)

        # add file name
        blob_name += filename

        print(blob_name)
        return blob_name
    
    
 
def resolve_partitions(partitions):
        if isinstance(partitions, dict):
            return "/".join(f"{k}={v}" for k, v in partitions.items()) + "/"

        if isinstance(partitions, str):
            if partitions.endswith("/"):
                partitions = partitions[:-1]

            # If there is no partition
            if len(partitions) == 0:
                return ""

            # It should fail if there is folder which is not a partition
            try:
                # check if it fits rule
                {b.split("=")[0]: b.split("=")[1] for b in partitions.split("/")}
            except IndexError as e:
                raise Exception(
                    f"The path {partitions} is not a valid partition"
                ) from e

            return partitions + "/"

        raise Exception(f"Partitions format or type not accepted: {partitions}")

def upload_to_gcs(bucket_name, dataset_id, table_id, path):
    """
    Uploads files to the specified GCS bucket. Handles both individual files and directories, maintaining Hive partitioning format.

    Parameters:
    - bucket_name: Name of the GCS bucket.
    - dataset_id: Identifier for the dataset, used to create a folder in the bucket.
    - table_id: Identifier for the table, used to create a folder within the dataset folder.
    - path: Path to the file or directory to upload.
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    path = Path(path)

    if path.is_dir():
        #extrai path 
        paths = [
            f
            for f in path.glob("**/*")
            if f.is_file() and f.suffix in [".csv", ".parquet", "parquet.gzip"]
        ]

        #extrai partes do path (partições)
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
    
    #TODO: IMPLEMENT CHUNKSEIZE OPTION TO RESILIENT DOWNLOAD
        
    for filepath, part in tqdm(list(zip(paths, parts)), desc="Uploading files"):
        #print(str(filepath))  # convert filepath to string
        blob_name = build_blob_name(
            mode='staging', 
            #usa atribuito name do objeto Path, que é o nome do arquivo
            filename=filepath.name, 
            table_id=table_id,   
            dataset_id=dataset_id, 
            partitions=part)  # convert filepath to string
       
        blob = bucket.blob(blob_name)
        #filepath é um objeto tipo Path, então é necessário converter para string
        blob.upload_from_filename(str(filepath))



def create_dataset_and_table_with_inferred_schema(project_id, dataset_id, table_id, bucket_name):
    """
    Creates a dataset and table in BigQuery, infers the schema from a file in GCS, and loads all files data.

    Parameters:
    - project_id: GCP project ID.
    - dataset_id: BigQuery dataset ID.
    - table_id: BigQuery table ID.
    - bucket_name: Name of the GCS bucket.
    """
    source_folder_path = f"staging/{dataset_id}/{table_id}/"
    
    client = bigquery.Client(project=project_id)
    staging_dataset_id = f"{dataset_id}_staging"
    # Listar arquivos no diretório especificado
    blob_names = list_blobs_in_folder(bucket_name, source_folder_path)
    if not blob_names:
        raise ValueError(f"No files found in gs://{bucket_name}/{source_folder_path}.")

    # Escolher o primeiro arquivo para inferência de esquema
    blob_name = blob_names[0]
    print(f"Inferring schema from file: {blob_name}")

    # Inferir esquema
    schema = infer_schema_from_file(bucket_name, blob_name)

    # Criar Dataset
    dataset_ref = bigquery.DatasetReference(project_id, staging_dataset_id)
    dataset = bigquery.Dataset(dataset_ref)
    dataset = client.create_dataset(dataset, exists_ok=True)
    print(f"Dataset {staging_dataset_id} created.")

    # Criar Tabela com o esquema inferido
    table_ref = dataset_ref.table(table_id)
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        autodetect=False, 
        source_format=bigquery.SourceFormat.CSV if '.csv' in blob_name else bigquery.SourceFormat.PARQUET
    )

    
    # Gerar URIs para todos os arquivos
    source_uris = [f"gs://{bucket_name}/staging/{dataset_id}/{table_id}/{name.split('/')[-1]}" for name in blob_names]
    
    print(f'Loading {len(source_uris)} files from {source_folder_path} into {table_id}...')
    
    load_job = client.load_table_from_uri(source_uris, table_ref, job_config=job_config)

    # Aguardar a conclusão do trabalho de carga
    load_job.result()

    print(f"Table {table_id} created and data loaded with inferred schema.")



def infer_schema_from_file(bucket_name, blob_name,extra_columns=[]):
    """
    Infers the schema of a file (CSV or Parquet) stored in GCS.

    Parameters:
    - bucket_name: Name of the GCS bucket.
    - blob_name: Name of the blob (file) in the bucket.

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

def list_blobs_in_folder(bucket_name, folder_path):
    """
    Lists all blobs in the specified bucket that are in the specified directory.
    
    Parameters:
    - bucket_name: Name of the GCS bucket.
    - folder_path: Path of the folder within the bucket. Make sure it ends with '/'.
    
    Returns:
    A list of blob names found in the folder.
    """
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name, prefix=folder_path)
    blob_names = [blob.name for blob in blobs if not blob.name.endswith('/')]

    return blob_names


