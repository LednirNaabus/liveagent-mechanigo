import asyncio
import logging
import pandas as pd
from config import config
from core.liveagent import LiveAgentClient
from utils.bq_utils import generate_schema, load_data_to_bq, sql_query_bq, drop_table_bq

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

async def extract_and_load_users(table_name: str):
    try:
        async with LiveAgentClient(config.API_KEY) as client:
            success, response = await client.ping()
            if success:
                table = "users_tmp"
                query = f"""
                SELECT
                DISTINCT(id), name, email, role, avatar_url
                FROM `{config.GCLOUD_PROJECT_ID}.{config.BQ_DATASET_NAME}.{table}`
                """
                users_df = sql_query_bq(query)
                logging.info("Generating schema and loading data to BigQuery...")
                schema = generate_schema(users_df)
                load_data_to_bq(
                    users_df,
                    config.GCLOUD_PROJECT_ID,
                    config.BQ_DATASET_NAME,
                    table_name,
                    "WRITE_APPEND",
                    schema
                )
                drop_table_bq(config.GCLOUD_PROJECT_ID, config.BQ_DATASET_NAME, table)
                return users_df.to_dict(orient="records")
            else:
                logging.error(f"Ping to '{client.BASE_URL}/ping' failed. Response: {response}")
    except Exception as e:
        logging.error(f"Exception occured during extraction and loading of users: {e}")
        raise