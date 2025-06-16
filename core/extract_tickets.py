import os
import pytz
import json
import aiohttp
import logging
import pandas as pd
from tqdm import tqdm
from typing import Literal, Any
from datetime import datetime
from config import config
from core.liveagent import LiveAgentClient
from utils.df_utils import fill_nan_values
from utils.bq_utils import generate_schema, load_data_to_bq
from utils.date_utils import set_filter, set_timezone, format_date_col, FilterField

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

async def extract_and_load_tickets(date: pd.Timestamp, table_name: str, filter_field: FilterField = FilterField.DATE_CHANGED, per_page: int = 100) -> list[dict[str, Any]]:
    """
    """
    filters = set_filter(date, filter_field=filter_field)
    ticket_payload = {
        "_perPage": per_page,
        "_filters": filters
        # "_sortDir": "DESC" if filter_field == FilterField.DATE_CREATED else None
    }

    if filter_field == FilterField.DATE_CREATED:
        ticket_payload["_sortDir"] = "ASC"

    async with LiveAgentClient(config.API_KEY) as client:
        success, ping_response = await client.ping()
        try:
            if success:
                logging.info(f"Ping to '{client.BASE_URL}/ping' successful.")
                logging.info(f"Extracting using the following filter: {ticket_payload['_filters']}")
                tickets = await client.ticket.fetch_tickets(ticket_payload, 100)
                # Add datetime_extracted column
                tickets["datetime_extracted"] = pd.Timestamp.now().strftime("%Y-%m-%dT%H:%M:%S")
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
                # Normalize custom fields
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
                tickets = fill_nan_values(tickets)
                # For logging/debugging purposes (dev or local branch)
                if filter_field == FilterField.DATE_CREATED:
                    file_name = os.path.join('csv', f"tickets-{date.date()}.csv")
                    tickets.to_csv(file_name, index=False)
                return tickets.to_dict(orient="records")
            else:
                logging.error(f"Ping to '{client.BASE_URL}/ping' failed. Response: {ping_response}")
        except Exception as e:
            logging.error(f"Exception occured while extracting tickets: {e}")
            raise

async def extract_and_load_ticket_messages(table_name: str):
    pass