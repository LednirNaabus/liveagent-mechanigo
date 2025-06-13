import os
import pytz
import json
import aiohttp
import logging
import pandas as pd
from tqdm import tqdm
from config import config
from core.liveagent import LiveAgentClient
from utils.df_utils import fill_nan_values
from utils.bq_utils import generate_schema, load_data_to_bq

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

async def extract_tickets():
    filters = json.dumps([
        ["date_created", "D>=", "2025-01-01 00:00:00"]
    ])
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
                file_name = os.path.join('csv', 'tickets-2025-delete-later.csv')
                tickets = fill_nan_values(tickets)
                tickets.to_csv(file_name, index=False)
                return tickets.to_dict(orient="records")
            else:
                logging.error(f"Ping to '{client.BASE_URL}/ping' failed. Response: {ping_response}")
        except Exception as e:
            logging.error(f"Exception occured while fetching tickets: {e}")
            raise