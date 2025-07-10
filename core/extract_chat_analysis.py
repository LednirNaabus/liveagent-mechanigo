import time
import asyncio
import logging
import traceback
import pandas as pd
from tqdm import tqdm
from core.convodataextract import ConvoDataExtract
from utils.bq_utils import sql_query_bq, generate_schema, load_data_to_bq, create_table_bq
from utils.geocoding_utils import geocode, tag_viable
from utils.df_utils import drop_cols
from utils.date_utils import set_timezone, format_date_col
from config import config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

async def process_single_chat(ticket_id: str, date_extracted: str, semaphore: asyncio.Semaphore) -> pd.DataFrame:
    async with semaphore:
        try:
            logging.info(f"Ticket ID: {ticket_id}")
            processor = await ConvoDataExtract.create(ticket_id, api_key=config.OPENAI_API_KEY)
            tokens = processor.data.get('tokens')
            new_df = pd.DataFrame([processor.data.get('data')])
            tokens_df = pd.DataFrame([tokens], columns=['tokens'])
            combined = pd.concat([new_df, tokens_df], axis=1)
            combined['date_extracted'] = date_extracted
            combined['date_extracted'] = pd.to_datetime(combined['date_extracted'], errors='coerce')
            combined = set_timezone(combined, "date_extracted", target_tz=config.MNL_TZ)
            combined.insert(0, 'ticket_id', ticket_id)
            return combined
        except Exception as e:
            logging.error(f"Exception occurred while processing single chat: {e}")
            return pd.DataFrame()

async def process_chat(ticket_ids: pd.Series):
    date_extracted = pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%d %H:%M:%S")
    semaphore = asyncio.Semaphore(5)
    tasks = [
        process_single_chat(ticket_id, date_extracted, semaphore)
        for ticket_id in ticket_ids['ticket_id']
    ]
    results = await asyncio.gather(*tasks)
    return pd.concat(results, ignore_index=True)

def process_address(df: pd.Series):
    locations = []
    for i in tqdm(df['location'], desc="Processing geolocation..."):
        try:
            result = geocode(address=i)
            tqdm.write(f"Result: {result}")
            if result is None:
                result = {'lat': None, 'lng': None, 'address': i}
        except AttributeError as a:
            print(f"Attribute Error occured: {a}")
        except Exception as e:
            result = {'lat': None, 'lng': None, 'address': i, 'error': str({e})}
        locations.append(result)
    return pd.DataFrame(locations)

def merge(table_name: str, df: pd.DataFrame) -> None:
    logging.info("Generating schema and loading data into BigQuery...")
    schema = generate_schema(df)
    create_table_bq(
        config.GCLOUD_PROJECT_ID,
        config.BQ_DATASET_NAME,
        table_name,
        schema
    )
    # load data to convo analysis history
    # For historical data purposes
    history = f"{table_name}_history"
    logging.info(f"Loading data to conversation analysis history table: {history}")
    load_data_to_bq(
        df,
        config.GCLOUD_PROJECT_ID,
        config.BQ_DATASET_NAME,
        history,
        "WRITE_APPEND",
        schema
    )
    # Load data to staging table
    staging_table = f"{table_name}_staging"
    logging.info(f"Loading data to a staging table: {staging_table}")
    load_data_to_bq(
        df,
        config.GCLOUD_PROJECT_ID,
        config.BQ_DATASET_NAME,
        staging_table,
        "WRITE_TRUNCATE",
        schema
    )
    columns = [
        'service_category', 'summary', 'intent_rating', 'engagement_rating', 'clarity_rating',
        'resolution_rating', 'sentiment_rating', 'location', 'schedule_date', 'schedule_time',
        'car', 'inspection', 'quotation', 'tokens', 'date_extracted',
        'address', 'viable', 'model'
    ]
    all_columns = ['ticket_id'] + columns
    update_set_clause = ',\n    '.join([f"{col} = source.{col}" for col in columns])
    insert_columns = ', '.join(all_columns)
    insert_values = ', '.join([f"source.{col}" for col in all_columns])
    # Perform merge operation
    logging.info("Merging...")
    merge_query=f"""
    MERGE `{config.GCLOUD_PROJECT_ID}.{config.BQ_DATASET_NAME}.{table_name}` AS target
    USING `{config.GCLOUD_PROJECT_ID}.{config.BQ_DATASET_NAME}.{staging_table}` AS source
    ON target.ticket_id = source.ticket_id
    WHEN MATCHED THEN
        UPDATE SET
            {update_set_clause}
    WHEN NOT MATCHED THEN
        INSERT ({insert_columns})
        VALUES ({insert_values})
    """
    sql_query_bq(merge_query, return_data=False)
    drop = """DROP TABLE `{}.{}.{}`""".format(config.GCLOUD_PROJECT_ID, config.BQ_DATASET_NAME, staging_table)
    sql_query_bq(drop, return_data=False)

def query_tickets() -> tuple:
    # Date filtering
    now = pd.Timestamp.now(tz="UTC").astimezone(config.MNL_TZ)
    date = now - pd.Timedelta(hours=6)
    start = date.floor('h')
    end = start + pd.Timedelta(hours=6) - pd.Timedelta(seconds=1)
    start_str = start.strftime("%Y-%m-%d %H:%M:%S")
    end_str = end.strftime("%Y-%m-%d %H:%M:%S")
    # Querying
    query = f"""
    SELECT
    DISTINCT ticket_id
    FROM {config.GCLOUD_PROJECT_ID}.{config.BQ_DATASET_NAME}.messages
    WHERE message_format = 'T'
        AND datecreated BETWEEN '{start_str}' AND '{end_str}'
    """
    return query, start_str, end_str

async def chat_analysis(results: pd.DataFrame) -> pd.DataFrame:
    logging.info(f"DF: {results}")
    logging.info(f"DF['ticket_id']: {results['ticket_id']}")
    ticket_messages_df = await process_chat(ticket_ids=results)
    logging.info("Done processing chats.")
    logging.info(f"Head: {ticket_messages_df.head()}")
    logging.info("Finding geolocation...")
    geolocation = process_address(ticket_messages_df)
    logging.info(f"Head: {geolocation.head()}")
    ticket_messages_df = pd.concat([ticket_messages_df, geolocation], axis=1)
    # Add 'viable' column
    ticket_messages_df = tag_viable(ticket_messages_df)
    # Dropping columns that are not needed
    ticket_messages_df = drop_cols(ticket_messages_df, "score", "input_address", "lat", "lng", "error")
    return ticket_messages_df

async def extract_and_load_chat_analysis(table_name: str):
    try:
        query, start_str, end_str = query_tickets()
        logging.info(f"Querying using: {query}")

        # Handle case where query results is None
        try:
            results = sql_query_bq(query)
            logging.info(f"First 5 results: {results.head()}")
            if results.empty:
                logging.warning(f"No messages found between {start_str} and {end_str}. Skipping the analysis...")
                return None
            logging.info(f"First 5 results: {results.head()}")
        except Exception as e:
            logging.error(f"Query failed: {e}")
            return None

        # Processing
        start_time = time.perf_counter()
        ticket_messages_df = await chat_analysis(results)
        merge(table_name, ticket_messages_df)
        end_time = time.perf_counter()
        elapsed_time = end_time - start_time
        logging.info(f"Time elapsed during conversation analysis: {elapsed_time:.2f} seconds.")
        logging.info(f"Processed chats and Geolocation:\n{ticket_messages_df.head()}")
        # Make date columns JSON serializable to avoid error/warning
        ticket_messages_df = format_date_col(
            ticket_messages_df,
            "date_extracted"
        )
        return ticket_messages_df.to_dict(orient="records")
    except Exception as e:
        logging.error(f"Exception occured while processing chat analysis: {e}")
        traceback.print_exc()
        raise