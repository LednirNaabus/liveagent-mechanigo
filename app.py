import os
import logging
import traceback
import pandas as pd
from typing import Optional
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from config import config
from utils.date_utils import FilterField, set_timezone
from utils.extract_utils import initial_extract, scheduled_extract
from utils.bq_utils import sql_query_bq, generate_schema, load_data_to_bq
from core.extract_tags import extract_and_load_tags
from core.extract_tickets import extract_and_load_tickets, extract_and_load_ticket_messages
from core.extract_agents import extract_and_load_agents
from core.extract_users import extract_and_load_users
from core.extract_chat_analysis import extract_and_load_chat_analysis
from core.extraction_log import extract_and_load_logs

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

app = FastAPI()

def record_start_time(now: pd.Timestamp):
    start_time_log = pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%d %H:%M:%S")
    logging.info(f"Start Time: {start_time_log}")
    now_df = pd.DataFrame({
        "batch_date": [now.date().isoformat()],
        "start_timestamp": [start_time_log]
    })
    now_df["start_timestamp"] = pd.to_datetime(now_df["start_timestamp"], errors="coerce")
    now_df = set_timezone(
        now_df,
        "start_timestamp",
        target_tz=config.MNL_TZ
    )
    logging.info("Generating schema and loading data into BigQuery...")
    schema = generate_schema(now_df)
    load_data_to_bq(
        now_df,
        config.GCLOUD_PROJECT_ID,
        config.BQ_DATASET_NAME,
        "extraction_metadata",
        "WRITE_APPEND",
        schema
    )

@app.get("/")
def root():
    """
    Home root path - for testing purposes.
    """
    return {"message": "Hello World!"}

@app.post("/mechanigo-liveagent/update-agents/{table_name}")
async def update_agents(table_name: str):
    """
    End point to update and fetch agents from the LiveAgent API on a daily basis (will be run by a cloud scheduler).

    This endpoint performs the following actions:
    1. Retrieves the agents from the LiveAgent API via the `/agents` endpoint.
    2. Loads the fetched agents data into a specified BigQuery table.

    The `table_name` parameter is used to determine which BigQuery table the data should be loaded into.

    Returns:

        - A JSON response containing the fetched agents data if successful.

        - A JSON response with an error message and status if an exception occurs during the process.

    Args:

        - `table_name` (`str`) : The name of the BigQuery table where the agents data will be stored.

    Raises:

        Exception: Any error encountered during the extraction or loading process will be captured and returned in the response.
    """
    try:
        logging.info("Extracting and loading agents...")
        agents = await extract_and_load_agents(table_name)
        return JSONResponse(agents)
    except Exception as e:
        logging.error(f"Exception occured while updating tickets: {e}")
        return JSONResponse(content={
            'error': str(e),
            'status': 'error'
        })

@app.post("/mechanigo-liveagent/update-users/{table_name}")
async def update_users(table_name: str):
    """
    Endpoint to update and fetch users from the LiveAgent API (used in development and testing only). See `core.liveagent.py` for extraction and loading of users.

    This endpoint performs the following actions:
    1. Retrieves the users data from the LiveAgent API via the `/users/{userID}` endpoint.
    2. Loads the fetched users data into a specified BigQuery table.

    Returns:
    
        - A JSON response containing the fetched users if successful.

        - A JSON response with an error message and status if an exception occurs during the process.

    Args:

        `table_name` (`str`) : The name of the BigQuery table where the data will be stored.
    
    Raises:

        Exception: Any error encountered during the extraction or loading process will be captured and returned in the response.
    """
    try:
        logging.info("Extracting and loading users...")
        users = await extract_and_load_users(table_name)
        return JSONResponse(users)
    except Exception as e:
        logging.error(f"Exception occured while updating users: {e}")
        return JSONResponse(content={
            'error': str(e),
            'status': 'error'
        })

@app.post("/mechanigo-liveagent/update-tags/{table_name}")
async def update_tags(table_name: str):
    """
    Endpoint to update and fetch tags from the LiveAgent API on a daily basis (will be run by a cloud scheduler).

    This endpoint performs the following actions:
    1. Retrieves the tags data from the LiveAgent API via the `/tags` endpoint.
    2. Loads the fetched tags data into a specified BigQuery table.

    Returns:

        - A JSON response containing the fetched tags data if successful.

        - A JSON response with an error message and status if an exception occurs during the process.

    Args:
        `table_name` (`str`) : The name of the BigQuery table where the tags data will be stored.
    
    Raises:

        Exception: Any error encountered during the extraction or loading process will be captured and returned in the response.
    """
    try:
        tags = await extract_and_load_tags(table_name)
        return JSONResponse(tags)
    except Exception as e:
        logging.error(f"Exception occured while updating tickets: {e}")
        return JSONResponse(content={
            'error': str(e),
            'status': 'error'
        })

@app.post("/mechanigo-liveagent/update-tickets/{table_name}")
async def update_tickets(
    table_name: str,
    is_initial: bool = Query(False),
    date: Optional[str] = Query(None, description="Optional date in YYYY-MM-DD format. **Important**: Date should be start of month (i.e., 2025-01-01, or 2025-12-01, etc.)")
):
    """
    Endpoint to update and fetch tickets from the LiveAgent API on a daily basis (wll be run by a cloud scheduler).

    This endpoint performs the following actions:
    1. Retrieves the tickets data from the LiveAgent API via the `/tickets` endpoint.
    2. Loads the fetched tickets data into a specified BigQuery table.
    """
    try:
        if is_initial:
            logging.info("Running initial ticket extraction...")
            if date:
                date = pd.Timestamp(date)
            else:
                date = pd.Timestamp("2025-05-01") # Manually change
            logging.info(f"Date to be extracted: {date}")
            tickets = await extract_and_load_tickets(date, table_name, filter_field=FilterField.DATE_CREATED)
        else:
            now = pd.Timestamp.now(tz="UTC").astimezone(config.MNL_TZ)
            date = now - pd.Timedelta(hours=6)
            logging.info(f"Date and time of execution: {date}")
            # Record start time - for logging
            record_start_time(now=now)
            tickets = await extract_and_load_tickets(date, table_name, filter_field=FilterField.DATE_CHANGED)
        return JSONResponse(tickets)
    except Exception as e:
        logging.error(f"Exception occured while updating tickets: {e}")
        return JSONResponse(content={
            'error': str(e),
            'status': 'error'
        })

@app.post("/mechanigo-liveagent/update-ticket-messages/{table_name}")
async def update_ticket_messages(
    table_name: str,
    is_initial: bool = Query(False),
    date: Optional[str] = Query(None, description="Date you want to extract (YYYY-MM-DD).")
):
    """
    """
    try:
        tickets_table_name = "tickets"
        if is_initial:
            logging.info("Running initial ticket extraction. For backlog purposes.")
            if date:
                query = initial_extract(date, tickets_table_name)
            else:
                # For backlog
                query = initial_extract("2025-01-01", tickets_table_name)
            logging.info(f"Date to be extracted: {date}")
            tickets_df = sql_query_bq(query)
            messages = await extract_and_load_ticket_messages(tickets_df, table_name, 10)
        else:
            logging.info(f"About to execute query. Time now: {pd.Timestamp.now(tz='UTC')}")
            query_str = scheduled_extract(tickets_table_name)
            tickets_df = sql_query_bq(query_str)
            messages = await extract_and_load_ticket_messages(tickets_df, table_name, 100)
        return JSONResponse(messages)
    except Exception as e:
        logging.error(f"Exception occurred while updating ticket messsages: {e}")
        traceback.print_exc()
        return JSONResponse(content={
            'error': str(e),
            'status': 'error'
        })

@app.post("/mechanigo-liveagent/update-chat-analysis/{table_name}")
async def update_chat_analysis(table_name: str):
    try:
        chat_analysis = await extract_and_load_chat_analysis(table_name)
        if chat_analysis is None:
            logging.info("No chat data to process.")
            return JSONResponse(content={
                'message': "No data found for specifed date or time range.",
                'status': 'error'
            })
        return JSONResponse(chat_analysis)
    except Exception as e:
        logging.error(f"Exception occured while updating chat analysis: {e}")
        return JSONResponse(content={
            'error': str(e),
            'status': 'error'
        })

@app.post("/mechanigo-liveagent/extract-logs/{table_name}")
async def extract_log(table_name: str):
    try:
        now = pd.Timestamp.now(tz=config.MNL_TZ)
        logs = extract_and_load_logs(now, table_name)
        return JSONResponse(logs)
    except Exception as e:
        logging.error(f"Exception occurred while extracting logs: {e}")
        return JSONResponse(content={
            'error': str(e),
            'status': 'error'
        })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get('PORT', 8080)))