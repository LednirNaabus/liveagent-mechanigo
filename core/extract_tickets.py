import os
import pytz
import json
import aiohttp
import logging
import pandas as pd
from tqdm import tqdm
from datetime import datetime
from config import config
from core.liveagent import LiveAgentClient
from utils.df_utils import fill_nan_values
from utils.bq_utils import generate_schema, load_data_to_bq
from utils.date_utils import set_filter, set_timezone, format_date_col

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

async def extract_tickets(date: pd.Timestamp, table_name: str):
    filters = set_filter(date)
    ticket_payload = {
        "_perPage": 10,
        "_filters": filters
    }
    async with LiveAgentClient(config.API_KEY) as client:
        success, ping_response = await client.ping()
        try:
            if success:
                logging.info(f"Ping to {client.BASE_URL} successful.")
                tickets = await client.ticket.fetch_tickets(ticket_payload, 1)
                # Add datetime_extracted column
                tickets["datetime_extracted"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
                tickets["datetime_extracted"] = pd.to_datetime(tickets["datetime_extracted"], errors="coerce")
                # Set timezone
                tickets = set_timezone(
                    tickets,
                    "date_created",
                    "date_changed",
                    "last_activity",
                    "last_activity_public",
                    "date_due",
                    "date_deleted",
                    "date_resolved",
                    target_tz=pytz.timezone("Asia/Manila")
                )
                # Normalize df['custom_fields'] - must be appropriate pyarrow datatype
                tickets["custom_fields"] = tickets["custom_fields"].apply(
                    lambda x: x[0] if isinstance(x, list) and len(x) == 1 and isinstance(x[0], dict) else None
                )
                logging.info("Generating schema and loading data to BigQuery...")
                schema = generate_schema(tickets)
                load_data_to_bq(
                    tickets,
                    config.GCLOUD_PROJECT_ID,
                    config.BQ_DATASET_NAME,
                    table_name,
                    "WRITE_APPEND",
                    schema
                )
                # Make date columns JSON serializeable
                tickets = format_date_col(
                    tickets,
                    "date_created",
                    "date_changed",
                    "last_activity",
                    "last_activity_public",
                    "date_due",
                    "date_deleted",
                    "date_resolved",
                    "datetime_extracted"
                )
                # Fill NaN values
                tickets = fill_nan_values(tickets)
                return tickets.to_dict(orient="records")
            else:
                logging.error(f"Ping to '{client.BASE_URL}/ping' failed. Response: {ping_response}")
        except Exception as e:
            logging.error(f"Exception occured while fetching tickets: {e}")
            raise

async def extract_ticket_messages(table_name: str):
    pass