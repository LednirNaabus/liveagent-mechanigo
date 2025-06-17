import asyncio
import logging
import pandas as pd
from config import config
from core.liveagent import LiveAgentClient
from utils.bq_utils import generate_schema, load_data_to_bq, sql_query_bq

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

async def extract_and_load_users(table_name: str):
    try:
        async with LiveAgentClient(config.API_KEY) as client:
            success, response = await client.ping()
            if success:
                table = "messages"
                query = f"""
                SELECT DISTINCT(userid)
                FROM `{config.GCLOUD_PROJECT_ID}.{config.BQ_DATASET_NAME}.{table}`
                """
                messages = sql_query_bq(query)
                user_ids = messages['userid'].tolist()
                print(user_ids)
                users = await asyncio.gather(*(client.get_user(user_id) for user_id in user_ids))
                users_df = pd.concat(users, ignore_index=True)
                logging.info("Generating schema and loading data to BigQuery...")
                schema = generate_schema(users_df)
                load_data_to_bq(
                    users_df,
                    config.GCLOUD_PROJECT_ID,
                    config.BQ_DATASET_NAME,
                    table_name,
                    "WRITE_TRUNCATE",
                    schema
                )
                return users_df.to_dict(orient="records")
            else:
                logging.error(f"Ping to '{client.BASE_URL}/ping' failed. Response: {response}")
    except Exception as e:
        logging.error(f"Exception occured during extraction and loading of users: {e}")
        raise