from prefect import flow
import os
from prefect_pipelines.brazil_bcb_sicor.constants import Constants as bcb_contants
from prefect_pipelines.brazil_bcb_sicor.task import (
    upload_to_gcs_task,
    create_dataset_and_table_with_inferred_schema_task,
    download_files_task,
    pre_process_files_task,
    run_dbt_model_task,
    )




BUCKET = os.environ.get("GCP_GCS_BUCKET", "de-zoomcamp-2k24")
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/gabri/brazil-rural-credit/credentials_dezoomcamp.json"
PROJECT_ID = 'de-zoomcamp-2k24'
DATASET_ID = 'brazil_rural_credit'
TABLE_ID = 'microdados_operacao_test'
PATH_DATA = f'/home/gabri/brazil-rural-credit/tmp/output/microdados_2013.csv'


@flow
def brazil_bcb_sicor_microdados_operacoes_flow():
    
    download_files = download_files_task(
        urls=bcb_contants.URLS.value['microdados_operacao'],
        folder=bcb_contants.INPUT_FOLDER.value,
        max_parallel=5,
    )
    
    pre_process_files = pre_process_files_task(
        folder=bcb_contants.INPUT_FOLDER.value,
        output_folder=bcb_contants.OUTPUT_FOLDER.value,
        table_id='microdados_operacao',
    )
    
    etapa = upload_to_gcs_task(
        bucket_name=BUCKET,
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        path=bcb_contants.OUTPUT_FOLDER.value,
    )
    
    etapa2 = create_dataset_and_table_with_inferred_schema_task(
        project_id=PROJECT_ID,
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        bucket_name=BUCKET,
    )
    
    run_dbt_model_task(model_name='brazil_rural_credit__microdados_operacao.sql')


brazil_bcb_sicor_microdados_operacoes_flow()