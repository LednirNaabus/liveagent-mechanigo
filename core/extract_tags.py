import logging
from config import config
from utils.df_utils import fill_nan_values
from utils.bq_utils import generate_schema, load_data_to_bq
from core.liveagent import LiveAgentClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

async def extract_and_load_tags(table_name: str):
    """
    Function to extract and load tags to BigQuery.

    Calls `fetch_tags()` from the `LiveAgentClient` class.
    """
    try:
        async with LiveAgentClient(config.API_KEY) as client:
            success, response = await client.ping()
            if success:
                tags = await client.fetch_tags()
                tags = fill_nan_values(tags)
                logging.info("Generating schema and loading data to BigQuery...")
                schema = generate_schema(tags)
                load_data_to_bq(
                    tags,
                    config.GCLOUD_PROJECT_ID,
                    config.BQ_DATASET_NAME,
                    table_name,
                    "WRITE_APPEND",
                    schema
                )
                return tags.to_dict(orient="records")
            else:
                logging.error(f"Ping to {client.BASE_URL} failed. Response: {response}")
    except Exception as e:
        logging.error(f"Exception occured during extraction and loading of tags: {e}")
        raise