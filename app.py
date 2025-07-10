import os
import json
import logging
import traceback
import pandas as pd
from typing import Optional, List, Dict
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
logger = logging.getLogger(__name__)

app = FastAPI()

ERROR_FILE = 'route_errors.json'
route_errors = []

def load_errors() -> List[Dict]:
    if os.path.exists(ERROR_FILE):
        try:
            with open(ERROR_FILE, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            return []
    return []

def save_errors(errors: List[Dict]):
    try:
        with open(ERROR_FILE, 'w') as f:
            json.dump(errors, f, default=str)
    except Exception as e:
        logger.error(f"Failed to save errors: {e}")

def add_error(route_name: str, error: Exception):
    errors = load_errors()
    error_info = {
        "route": route_name,
        "error_type": type(error).__name__,
        "error_message": str(error)
    }
    errors.append(error_info)
    save_errors(errors)
    logger.error(f"Error in {route_name}: {error}")

def get_and_clear_errorrs() -> List[Dict]:
    errors = load_errors()
    if os.path.exists(ERROR_FILE):
        os.remove(ERROR_FILE)

    return errors

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
    return {"message": "Hello World!"}

@app.post("/mechanigo-liveagent/update-agents/{table_name}")
async def update_agents(table_name: str):
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
        add_error("/update-tickets", e)
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
        add_error("/update-ticket-messages", e)
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
        add_error("/update-chat-analysis", e)
        return JSONResponse(content={
            'error': str(e),
            'status': 'error'
        })

@app.post("/mechanigo-liveagent/extract-logs/{table_name}")
async def extract_log(table_name: str):
    try:
        now = pd.Timestamp.now(tz=config.MNL_TZ)
        collected_errors = get_and_clear_errorrs()
        logging.info(f"Extracting logs...")
        route_errors.clear() # clear global list for next cycle
        logs = extract_and_load_logs(now, table_name, errors=collected_errors)
        return JSONResponse(logs)
    except Exception as e:
        logging.error(f"Exception occurred while extracting logs: {e}")
        return JSONResponse(content={
            'error': str(e),
            'status': 'error'
        })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get('PORT', 8080)))