from prefect import task
import asyncio

from prefect_pipelines.brazil_bcb_sicor.utils import (
    upload_to_gcs, 
    create_dataset_and_table_with_inferred_schema,
    download_files,
    pre_process_files,
    run_dbt_model,
    )





# #TODO: create download folder

# #NOTE: Crawler task
# # Execução em ambiente assíncrono
# #await main(urls)  # Para Jupyter Notebook/IPython
# print(f'----- downloading data {urls[TABLE_ID]}')
# main(urls[TABLE_ID])  # Para script Python regular

#task do async download files and save em to a folder
#just a task that downloads

@task
def download_files_task(urls, folder, max_parallel=5):
    
    return asyncio.run(download_files(urls=urls, folder=folder, max_parallel=max_parallel))

#pre-process files read csvs 2 chunks save to parquet

@task
def pre_process_files_task(folder, output_folder, table_id):
    
    return pre_process_files(input_path=folder, output_path=output_folder, table_id=table_id)

#--- upload data to Storage
@task
def upload_to_gcs_task(bucket_name, dataset_id, table_id, path):
    
    return upload_to_gcs(
        bucket_name=bucket_name,
        dataset_id=dataset_id,
        table_id=table_id,
        path=path,
    )
    

#--- upload data to Big Query
@task
def create_dataset_and_table_with_inferred_schema_task(project_id, dataset_id, table_id, bucket_name):
    
    return create_dataset_and_table_with_inferred_schema(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name=bucket_name,
    )
    
@task
def run_dbt_model_task(model_name: str):
    
    return run_dbt_model(
        model_name=model_name
    )

