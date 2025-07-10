"""
EXTRACTION LOG SCHEMA
extraction_date, "DATETIME", i.e., 2025-07-08T15:02:33
extraction_run_time, "FLOAT" i.e., 127.22, etc.
no_tickets_new, "INTEGER"
no_tickets_update, "INTEGER"
no_tickets_total, "INTEGER" (new+update)
no_messages_new, "INTEGER"
no_messages_old, "INTEGER"
no_messages_total, "INTEGER" (new+old)
total_tokens, "INTEGER"
model, "STRING"
log_message, "STRING"
"""
from utils.bq_utils import sql_query_bq, generate_schema, load_data_to_bq
from utils.date_utils import get_start_end_str, format_date_col, set_timezone
from utils.df_utils import fill_nan_values
from config import config
from datetime import timedelta
from enum import StrEnum
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

class Tables(StrEnum):
    TICKETS = "tickets"
    MESSAGES = "messages"

def get_from_run(date: pd.Timestamp, table: Tables):
    if table == Tables.MESSAGES:
        id = "DISTINCT ticket_id"
    else:
        id = "id"

    start_str, end_str = get_start_end_str(date)
    query = """
    SELECT {}
    FROM {}.{}.{}
    WHERE datetime_extracted >= '{}' AND datetime_extracted < '{}'
    """.format(id, config.GCLOUD_PROJECT_ID, config.BQ_DATASET_NAME, table, start_str, end_str)
    print(query)
    return sql_query_bq(query)

def get_existing(table: Tables):
    logging.info(f"Table name: {table}")
    if table == Tables.MESSAGES:
        id = "DISTINCT ticket_id"
    else:
        id = "id"
    query = """
    SELECT {}
    FROM {}.{}.{}
    """.format(id, config.GCLOUD_PROJECT_ID, config.BQ_DATASET_NAME, table)
    return sql_query_bq(query)

def get_total_tokens(date: pd.Timestamp):
    start_str, end_str = get_start_end_str(date)
    # Get total tokens from run
    query = """
    SELECT model, SUM(tokens) AS total_tokens
    FROM {}.{}.convo_analysis
    WHERE date_extracted >= '{}' AND date_extracted < '{}'
    GROUP BY model
    """.format(config.GCLOUD_PROJECT_ID, config.BQ_DATASET_NAME, start_str, end_str)
    print(query)
    convo_analysis_df = sql_query_bq(query)
    return convo_analysis_df["total_tokens"], convo_analysis_df["model"]

def get_runtime(date: pd.Timestamp):
    batch_date = date.date().strftime("%Y-%m-%d")
    query = """
    SELECT start_timestamp
    FROM {}.{}.extraction_metadata
    WHERE batch_date = '{}'
    ORDER BY start_timestamp DESC
    LIMIT 1
    """.format(config.GCLOUD_PROJECT_ID, config.BQ_DATASET_NAME, batch_date)
    res = sql_query_bq(query)
    start_time = res.iloc[0,0].replace(tzinfo=config.MNL_TZ)
    return round((date - start_time).total_seconds(), 2) if not res.empty else None

def extract_and_load_logs(date: pd.Timestamp, table_name: str, errors: list):
    try:
        logs_df = pd.DataFrame({
            "extraction_date": [pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%d %H:%M:%S")]
        })
        logs_df["extraction_date"] = pd.to_datetime(logs_df["extraction_date"], errors="coerce")
        logs_df = set_timezone(
            logs_df,
            "extraction_date",
            target_tz=config.MNL_TZ
        )
        run_time = get_runtime(date)
        logging.info(f"Date: {date}")
        # Get existing ticket IDs
        get_ticket_ids_from_run = get_from_run(date, Tables.TICKETS) # get ticket IDs from current run first
        logging.info("Getting existing...")
        existing_ticket_ids = get_existing(Tables.TICKETS)
        logs_df["no_tickets_new"] = get_ticket_ids_from_run["id"].map(lambda ticket_id: ticket_id not in existing_ticket_ids).sum()
        logs_df["no_tickets_update"] = get_ticket_ids_from_run["id"].map(lambda ticket_id: ticket_id in existing_ticket_ids).sum()
        logs_df["no_tickets_total"] = logs_df["no_tickets_new"] + logs_df["no_tickets_update"]
        # Get existing message IDs
        get_message_ids_from_run = get_from_run(date, Tables.MESSAGES)
        existing_message_ids = get_existing(Tables.MESSAGES)
        logs_df["no_messages_new"] = get_message_ids_from_run["ticket_id"].map(lambda msg_id: msg_id not in existing_message_ids).sum()
        logs_df["no_messages_old"] = get_message_ids_from_run["ticket_id"].map(lambda msg_id: msg_id in existing_message_ids).sum()
        logs_df["no_messages_total"] = logs_df["no_messages_new"] + logs_df["no_messages_old"]
        # Get total tokens
        logging.info("Getting total tokens...")
        logs_df["total_tokens"], logs_df["model"] = get_total_tokens(date)
        # Get error messages here
        if errors:
            logs_df["log_message"] = errors
        else:
            logs_df["log_message"] = "None"
        logs_df["extraction_run_time"] = run_time
        # Load to BigQuery
        logging.info("Generating schema and loading data to BigQuery...")
        schema = generate_schema(logs_df)
        load_data_to_bq(
            logs_df,
            config.GCLOUD_PROJECT_ID,
            config.BQ_DATASET_NAME,
            table_name,
            "WRITE_APPEND",
            schema
        )
        logs_df = format_date_col(
            logs_df,
            "extraction_date"
        )
        logs_df["extraction_run_time"] = logs_df["extraction_run_time"].apply(
            lambda x: str(timedelta(seconds=x.total_seconds()))
        )
        logs_df = fill_nan_values(logs_df)
        return logs_df.to_dict(orient="records")
    except Exception as e:
        logging.error(f"Exception occurred during extraction and loading of logs: {e}")
        raise