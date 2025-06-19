import logging
import pandas as pd
from config import config
from utils.bq_utils import generate_schema, load_data_to_bq
from utils.date_utils import set_timezone
from core.liveagent import LiveAgentClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

async def extract_and_load_agents(table_name: str):
    try:
        async with LiveAgentClient(config.API_KEY) as client:
            success, response = await client.ping()
            if success:
                agents = await client.agent.get_agents(100)
                logging.info(f"Fetched response of length {len(agents)}.")
                logging.info("Generating schema and loading data to BigQuery...")
                agents_df = pd.DataFrame(agents)
                agents_df = set_timezone(
                    agents_df,
                    "last_pswd_change",
                    target_tz=config.MNL_TZ
                )
                schema = generate_schema(agents_df)
                load_data_to_bq(
                    agents_df,
                    config.GCLOUD_PROJECT_ID,
                    config.BQ_DATASET_NAME,
                    table_name,
                    "WRITE_APPEND",
                    schema
                )
                return agents
            else:
                logging.error(f"Ping to {client.BASE_URL}/ping failed. Response: {response}")
    except Exception as e:
        logging.error(f"Exception occured during extraction and loading of tags: {e}")
        raise