import logging
import pandas as pd
from typing import Any
from config import config
from core.liveagent import LiveAgentClient
from utils.df_utils import fill_nan_values
from utils.date_utils import set_filter, set_timezone, format_date_col, FilterField
from utils.bq_utils import generate_schema, load_data_to_bq, sql_query_bq, create_table_bq

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

async def extract_and_load_tickets(date: pd.Timestamp, table_name: str, filter_field: FilterField = FilterField.DATE_CHANGED, per_page: int = 100) -> list[dict[str, Any]]:
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
                # Check if dataframe is empty
                if tickets.empty:
                    raise ValueError("DataFrame is empty!")
                else:
                    # Add datetime_extracted column
                    tickets["datetime_extracted"] = pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%dT%H:%M:%S")
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
                        "datetime_extracted",
                        target_tz=config.MNL_TZ
                    )
                    # Normalize custom fields
                    tickets["custom_fields"] = tickets["custom_fields"].apply(
                        lambda x: x[0] if isinstance(x, list) and len(x) == 1 and isinstance(x[0], dict) else None
                    )
                    logging.info("Generating schema and empty table...")
                    schema = generate_schema(tickets)
                    create_table_bq(
                        config.GCLOUD_PROJECT_ID,
                        config.BQ_DATASET_NAME,
                        table_name,
                        schema
                    )
                    # Load data to staging table
                    logging.info("Loading data to a staging table...")
                    staging_table_name = f"{table_name}_staging"
                    logging.info(f"Table staging name: {staging_table_name}")
                    load_data_to_bq(
                        tickets,
                        config.GCLOUD_PROJECT_ID,
                        config.BQ_DATASET_NAME,
                        staging_table_name,
                        "WRITE_TRUNCATE",
                        schema
                    )
                    # Check if ticket exists
                    update_columns = [
                        'owner_contactid', 'owner_email', 'owner_name', 'departmentid', 'agentid', 
                        'status', 'tags', 'code', 'channel_type', 'date_created', 'date_changed', 
                        'date_resolved', 'last_activity', 'last_activity_public', 'public_access_urlcode', 
                        'subject', 'custom_fields', 'date_due', 'date_deleted', 'datetime_extracted'
                    ]
                    all_columns = ['id'] + update_columns
                    update_set_clauses = ',\n    '.join([f"{col} = source.{col}" for col in update_columns])
                    insert_columns = ', '.join(all_columns)
                    insert_values = ', '.join([f"source.{col}" for col in all_columns])
                    # Perform Merge operation
                    logging.info("Merging...")
                    merge_query = f"""
                    MERGE `{config.GCLOUD_PROJECT_ID}.{config.BQ_DATASET_NAME}.{table_name}` AS target
                    USING `{config.GCLOUD_PROJECT_ID}.{config.BQ_DATASET_NAME}.{staging_table_name}` AS source
                    ON target.id = source.id
                    WHEN MATCHED THEN
                        UPDATE SET
                            {update_set_clauses}
                    WHEN NOT MATCHED THEN
                        INSERT ({insert_columns})
                        VALUES ({insert_values})
                    """
                    sql_query_bq(merge_query, return_data=False)
                    drop = f"""DROP TABLE `{config.GCLOUD_PROJECT_ID}.{config.BQ_DATASET_NAME}.{staging_table_name}`"""
                    sql_query_bq(drop, return_data=False)
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
                return tickets.to_dict(orient="records")
            else:
                logging.error(f"Ping to '{client.BASE_URL}/ping' failed. Response: {ping_response}")
        except ValueError as e:
            logging.error(f"Exception occurred while extracting tickets: {e}")
        except Exception as e:
            logging.error(f"Exception occurred while extracting tickets: {e}")
            raise

async def extract_and_load_ticket_messages(tickets_df: pd.DataFrame, table_name: str, per_page: int = 10):
    tickets_data = {
        "id": tickets_df['id'].tolist(),
        "owner_name": tickets_df['owner_name'].tolist(),
        "agentid": tickets_df['agentid'].tolist(),
    }

    async with LiveAgentClient(config.API_KEY, config.GCLOUD_PROJECT_ID, config.BQ_DATASET_NAME) as client:
        success, ping_response = await client.ping()
        try:
            if success:
                messages_df = await client.ticket.fetch_ticket_message(
                    tickets_data,
                    max_pages=100,
                    message_per_page=per_page,
                    insert_to_bq=False,
                    batch_size=500
                )
                # Check if dataframe is empty
                if messages_df.empty:
                    raise ValueError("DataFrame is empty!")
                else:
                    # Add datetime_extracted column
                    messages_df["datetime_extracted"] = pd.Timestamp.now().strftime("%Y-%m-%dT%H:%M:%S")
                    messages_df["datetime_extracted"] = pd.to_datetime(messages_df["datetime_extracted"], errors="coerce")
                    messages_df = set_timezone(
                        messages_df,
                        "datecreated",
                        "datefinished",
                        "message_datecreated",
                        "datetime_extracted",
                        target_tz=config.MNL_TZ
                    )
                    logging.info("Generating schema and loading data to BigQuery...")
                    messages_df["sort_order"] = pd.to_numeric(messages_df["sort_order"], errors="coerce").fillna(0).astype(int)
                    schema = generate_schema(messages_df)
                    load_data_to_bq(
                        messages_df,
                        config.GCLOUD_PROJECT_ID,
                        config.BQ_DATASET_NAME,
                        table_name,
                        "WRITE_APPEND",
                        schema
                    )
                    messages_df = format_date_col(
                        messages_df,
                        "datecreated",
                        "datefinished",
                        "message_datecreated",
                        "datetime_extracted"
                    )
                    messages_df = fill_nan_values(messages_df)
                    logging.info(f"Processed {len(messages_df)} messages")
                    logging.info(f"Found {len(client.unique_userids)} unique user IDs")
                    logging.info("Extracting unique users from extracted ticket messages...")
                    await client.populate_users_from_collected_ids(batch_size=50)
                return messages_df.to_dict(orient="records")
            else:
                logging.error(f"Ping to '{client.BASE_URL}/ping' failed. Response: {ping_response}")
        except ValueError as e:
            logging.error(f"Exception occurred while extracting ticket messages: {e}")
        except Exception as e:
            logging.error(f"Exception occurred while extracting ticket messages: {e}")
            raise