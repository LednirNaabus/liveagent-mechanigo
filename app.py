import os
import pytz
import logging
import pandas as pd
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from core.extract_tags import extract_and_load_tags
from core.extract_tickets import extract_and_load_tickets_incremental, extract_and_load_tickets_initial
from core.extract_agents import extract_and_load_agents

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

app = FastAPI()

@app.get("/")
def root():
    """
    Home root path - for testing purposes.
    """
    return {"message": "Hello World!"}

@app.post("/mechanigo-liveagent/update-agents/{table_name}")
async def update_agents(table_name: str):
    """
    """
    try:
        agents = await extract_and_load_agents(table_name)
        return JSONResponse(agents)
    except Exception as e:
        logging.error(f"Exception occured while updating tickets: {e}")
        return JSONResponse(content={
            'error': str(e),
            'status': 'error'
        })

@app.post("/mechanigo-liveagent/update-tags/{table_name}")
async def update_tags(table_name: str):
    """
    Endpoint to update and fetch tags from the LiveAgent API on a daily basis.

    This endpoint performs the following actions:
    1. Retrieves the tags data from the LiveAgent API via the `/tags` endpoint.
    2. Loads the fetched tags data into a specified BigQuery table.

    The `table_name` parameter is used to determine which BigQuery table the tags data should be loaded into.

    Returns:
        - A JSON response containing the fetched tags data if successful.
        - A JSON response with an error message and status if an exception occurs during the process.

    Args:
        table_name (str): The name of the BigQuery table where the tags data will be stored.
    
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
async def update_tickets(table_name: str, is_initial: bool = Query(False)):
    """
    """
    try:
        if is_initial:
            logging.info("Running initial ticket extraction...")
            date = pd.Timestamp("2025-01-01")
            tickets = await extract_and_load_tickets_initial(date, table_name)
            pass
        else:
            now = pd.Timestamp.now(tz="UTC").astimezone(pytz.timezone("Asia/Manila"))
            date = now - pd.Timedelta(hours=6)
            logging.info(f"Date and time of execution: {date}")
            tickets = await extract_and_load_tickets_incremental(date, table_name)
        return JSONResponse(tickets)
    except Exception as e:
        logging.error(f"Exception occured while updating tickets: {e}")
        return JSONResponse(content={
            'error': str(e),
            'status': 'error'
        })

@app.post("/mechanigo-liveagent/update-ticket-messages/{table_name}")
async def update_ticket_messages(table_name: str):
    """
    """
    try:
        return JSONResponse(table_name)
    except Exception as e:
        logging.error(f"Exception occured while updating tickets: {e}")
        return JSONResponse(content={
            'error': str(e),
            'status': 'error'
        })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get('PORT', 8080)))