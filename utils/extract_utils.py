import time
import pandas as pd
import logging
from typing import Optional
from config import config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

def initial_extract(date_str: Optional[str], tickets_table_name: str) -> tuple[pd.Timestamp, pd.Timestamp]:
    """
    Used for `/update-ticket-messages`, specifically when query parameter `is_initial` is `True`.
    """
    date = pd.Timestamp(date_str) if date_str else pd.Timestamp("2025-05-01")
    start = date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    end = (start + pd.offsets.MonthEnd(1)).replace(hour=23, minute=59, second=59)
    start_str = start.strftime("%Y-%m-%d %H:%M:%S")
    end_str = end.strftime("%Y-%m-%d %H:%M:%S")
    query = """
    SELECT id, owner_name, agentid, date_created, date_changed
    FROM `{}.{}.{}`
    WHERE date_created BETWEEN '{}' AND '{}'
    ORDER BY date_created
    LIMIT 100
    """.format(config.GCLOUD_PROJECT_ID, config.BQ_DATASET_NAME, tickets_table_name, start_str, end_str)
    logging.info(f"Query: {query}")
    return query

def scheduled_extract(tickets_table_name: str) -> str:
    """
    Used for `/update-ticket-messages`, specifically when query parameter `is_initial` is `False`.
    """
    now = pd.Timestamp.now(tz=config.MNL_TZ)
    now = pd.to_datetime(now, errors="coerce")
    logging.info(f"Now PH Time (timezone aware): {now}")
    logging.info(f"Now (in UTC) (timezone aware): {now.astimezone('UTC')}")
    date = now - pd.Timedelta(hours=6)
    logging.info(f"Date: {date}")
    start = date.floor('h')
    logging.info(f"Start: {start}")
    end = start + pd.Timedelta(hours=6)
    logging.info(f"End: {end}")
    logging.info(f"Date and time of execution: {now}")
    logging.info(f"System timezone: {time.tzname}")
    start_str = start.strftime("%Y-%m-%d %H:%M:%S")
    end_str = end.strftime("%Y-%m-%d %H:%M:%S")
    query = """
    SELECT id, owner_name, agentid, date_created, date_changed
    FROM `{}.{}.{}`
    WHERE date_created >= '{}' AND date_created < '{}'
    """.format(config.GCLOUD_PROJECT_ID, config.BQ_DATASET_NAME, tickets_table_name, start_str, end_str)
    logging.info(f"Query: {query}")
    return query