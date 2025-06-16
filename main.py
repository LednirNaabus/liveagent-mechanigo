import os
import json
import pytz
import asyncio
import pandas as pd
from config import config
from core.liveagent import LiveAgentClient
from utils.bq_utils import generate_schema, load_data_to_bq
from utils.date_utils import set_timezone

async def main():
    async with LiveAgentClient(config.API_KEY) as client:
        success, ping_response = await client.ping()
        if success:
            print(f"Ping to {client.BASE_URL} successful.")
            filters = json.dumps([
                ["date_created", "D>=", "2025-06-01 07:52:32"],
                ["date_created", "D<=", "2025-06-13 23:59:59"]
            ])
            ticket_payload = {
                "_perPage": 100,
                "_filters": filters
            }
            tickets = await client.ticket.fetch_tickets(ticket_payload,100)
            # ticket_messages = await client.ticket.fetch_ticket_message(tickets, 1)
            tickets["datetime_extracted"] = pd.Timestamp.now().strftime("%Y-%m-%dT%H:%M:%S")
            tickets["datetime_extracted"] = pd.to_datetime(tickets["datetime_extracted"], errors="coerce")

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
            tickets["custom_fields"] = tickets["custom_fields"].apply(
                lambda x: x[0] if isinstance(x, list) and len(x) == 1 and isinstance(x[0], dict) else None
            )
            # print("Generating schema and loading data to BigQuery...")
            # schema = generate_schema(tickets)
            # load_data_to_bq(
            #     tickets,
            #     config.GCLOUD_PROJECT_ID,
            #     config.BQ_DATASET_NAME,
            #     "tickets",
            #     "WRITE_APPEND",
            #     schema
            # )
            file_name = os.path.join('csv', "tickets-2025-delete-later-june01-13.csv")
            tickets.to_csv(file_name, index=False)
            # ticket_messages.to_csv(file_name, index=False)
            # print(ticket_messages)
            # ticket_messages = await client.ticket.fetch_ticket_message(tickets, 5)
            # print(ticket_messages)
            # tags = await client.fetch_tags()
            # print(tags)
            # agent = await client.agent.get_agents(100)
            # agent_df = pd.DataFrame(agent)
            # schema = generate_schema(agent_df)
            # load_data_to_bq(
            #     agent_df,
            #     config.GCLOUD_PROJECT_ID,
            #     config.BQ_DATASET_NAME,
            #     "table_delete_later",
            #     "WRITE_APPEND",
            #     schema
            # )
        else:
            print(f"API no response: {ping_response}")
            exit(1)

asyncio.run(main())