import logging
import pandas as pd
from typing import List
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.cloud.bigquery import SchemaField

from config import config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

def get_client():
    return {
        'client': config.BQ_CLIENT,
        'credentials': config.CREDS_FILE,
        'project_id': config.GCLOUD_PROJECT_ID
    }

def ensure_dataset(project_id: str, dataset_name: str, client: bigquery.Client):
    dataset_id = f"{project_id}.{dataset_name}"
    try:
        client.get_dataset(dataset_id)
    except NotFound:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "asia-southeast1"
        client.create_dataset(dataset, timeout=30)
        logging.error(f"Exception when checking dataset: {NotFound}")
        logging.info(f"Created dataset: {dataset_id}")

def ensure_table(project_id: str, dataset_name: str, table_name: str, client: bigquery.Client, schema=None):
    table_id = f"{project_id}.{dataset_name}.{table_name}"
    try:
        client.get_table(table_id)
        logging.info(f"Table {table_id} already exists!")
    except NotFound:
        table = bigquery.Table(table_id, schema=schema) if schema else bigquery.Table(table_id)
        client.create_table(table)
        logging.error(f"Exception when checking table: {NotFound}")
        logging.info(f"Created table: {table_id}")

def generate_schema(df: pd.DataFrame) -> List[SchemaField]:
    TYPE_MAPPING = {
        "i": "INTEGER",
        "u": "NUMERIC",
        "b": "BOOLEAN",
        "f": "FLOAT",
        "O": "STRING",
        "S": "STRING",
        "U": "STRING",
        "M": "DATETIME",
    }

    FORCE_NULLABLE = {"custom_fields"} # from /tickets
    schema = []
    for column, dtype in df.dtypes.items():
        val = df[column].iloc[0]
        is_list_of_dicts = isinstance(val, list) and len(val) > 0 and isinstance(val[0], dict)
        force_null = column in FORCE_NULLABLE
        mode = "NULLABLE" if force_null else ("REPEATED" if isinstance(val, list) else "NULLABLE")

        fields = ()
        if isinstance(val, dict):
            fields = generate_schema(pd.json_normalize(val))
        elif is_list_of_dicts:
            fields = generate_schema(pd.json_normalize(val))

        if fields:
            field_type = "RECORD"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            field_type = "DATETIME"
        else:
            field_type = TYPE_MAPPING.get(dtype.kind, "STRING")
        schema.append(
            SchemaField(
                name=column,
                field_type=field_type,
                mode=mode,
                fields=fields,
            )
        )
    return schema

def load_data_to_bq(df: pd.DataFrame, project_id: str, dataset_name: str, table_name: str, write_mode: str = "WRITE_APPEND", schema: bigquery.SchemaField = None):
    client = get_client()['client']
    ensure_dataset(project_id, dataset_name, client)
    ensure_table(project_id, dataset_name, table_name, client, schema)
    table_id = f"{project_id}.{dataset_name}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=write_mode,
        autodetect=schema is None,
    )
    try:
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        table = client.get_table(table_id)
        table.expires = None
        client.update_table(table, ["expires"])
        logging.info(f"Successfully loaded {df.shape[0]} rows into {table_id}.")
    except Exception as e:
        logging.error(f"Error uploading data to BigQuery: {e}")
        raise

def sql_query_bq(query: str, return_data: bool = True) -> pd.DataFrame:
    client = get_client()['client']
    query_job = client.query(query)
    if return_data:
        df = query_job.to_dataframe()
        return df
    else:
        query_job.result()
        return None

def drop_table_bq(project_id: str, dataset_name: str, table_name: str):
    client = get_client()['client']
    full_table_id = f"{project_id}.{dataset_name}.{table_name}"
    try:
        client.delete_table(full_table_id, not_found_ok=True)
        logging.info(f"{full_table_id} dropped.")
    except Exception as e:
        logging.error(f"Exception occurred while dropping table: {e}")
        logging.error(f"Failed to drop table '{full_table_id}'.")

def create_table_bq(project_id: str, dataset_name: str, table_name: str, schema: list[bigquery.SchemaField]=None):
    table_id = f"{project_id}.{dataset_name}.{table_name}"
    client = get_client()['client']
    try:
        client.get_table(table_id)
    except NotFound:
        logging.info(f"Table {table_id} does not exist. Creating one...")
        table = bigquery.Table(table_id, schema=schema) if schema else bigquery.Table(table_id)
        client.create_table(table)
        logging.info(f"Table {table_id} created.")
    except Exception as e:
        logging.error(f"Exception occurred while creating table: {e}")