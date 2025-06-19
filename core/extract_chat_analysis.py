import logging
import traceback
import pandas as pd
from tqdm import tqdm
from core.convodataextract import ConvoDataExtract
from utils.bq_utils import sql_query_bq, generate_schema, load_data_to_bq
from utils.geocoding_utils import geocode
from utils.df_utils import drop_cols
from config import config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

def process_chat(ticket_ids: pd.Series):
    rows = []
    date_extracted = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
    for ticket_id in ticket_ids['ticket_id']:
        logging.info(f"Ticket ID: {ticket_id}")
        processor = ConvoDataExtract(ticket_id, api_key=config.OPENAI_API_KEY)
        tokens = processor.data.get('tokens')
        new_df = pd.DataFrame([processor.data.get('data')])
        tokens_df = pd.DataFrame([tokens], columns=['tokens'])
        combined = pd.concat([new_df, tokens_df], axis=1)
        combined['date_extracted'] = date_extracted
        combined['date_extracted'] = pd.to_datetime(combined['date_extracted'], errors='coerce')
        combined.insert(0, 'ticket_id', ticket_id)
        rows.append(combined)
    return pd.concat(rows, ignore_index=True)

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

def extract_and_load_chat_analysis(table_name: str):
    try:
        # Date filtering
        now = pd.Timestamp.now(tz="UTC").astimezone(config.MNL_TZ)
        date = now - pd.Timedelta(hours=6)
        start = date.floor('h')
        end = start + pd.Timedelta(hours=6) - pd.Timedelta(seconds=1)
        start_str = start.strftime("%Y-%m-%d %H:%M:%S")
        end_str = end.strftime("%Y-%m-%d %H:%M:%S")
        # Querying
        query = f"""
        SELECT DISTINCT(ticket_id)
        FROM {config.GCLOUD_PROJECT_ID}.{config.BQ_DATASET_NAME}.messages
        WHERE datecreated BETWEEN '{start_str}' AND '{end_str}'
        """
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
        print(f"DF: {results}")
        print(f"DF['ticket_id']: {results['ticket_id']}")
        ticket_messages_df = process_chat(ticket_ids=results)
        logging.info("Done processing chats.")
        logging.info(f"Head: {ticket_messages_df.head()}")
        logging.info("Finding geolocation...")
        geolocation = process_address(ticket_messages_df)
        logging.info(f"Head: {geolocation.head()}")
        ticket_messages_df = pd.concat([ticket_messages_df, geolocation], axis=1)
        # Dropping columns that are not needed
        ticket_messages_df = drop_cols(ticket_messages_df, "score", "input_address", "lat", "lng", "error")
        logging.info("Generating schema and loading data into BigQuery...")
        schema = generate_schema(ticket_messages_df)
        load_data_to_bq(
            ticket_messages_df,
            config.GCLOUD_PROJECT_ID,
            config.BQ_DATASET_NAME,
            table_name,
            "WRITE_APPEND",
            schema
        )
        logging.info(f"Processed chats and Geolocation:\n{ticket_messages_df.head()}")
        return ticket_messages_df.to_dict(orient="records")
    except Exception as e:
        logging.error(f"Exception occured while processing chat analysis: {e}")
        traceback.print_exc()
        raise