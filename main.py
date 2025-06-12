import os
import json
import asyncio
import pandas as pd
from config import config
from core.liveagent import LiveAgentClient
from utils.bq_utils import generate_schema, load_data_to_bq

async def main():
    async with LiveAgentClient(config.API_KEY) as client:
        success, ping_response = await client.ping()
        if success:
            print(f"Ping to {client.BASE_URL} successful.")
            filters = json.dumps([
                ["date_changed", "D>=", "2025-01-01 00:00:00"]
            ])
            ticket_payload = {
                "_perPage": 10,
                "_filters": filters
            }
            tickets = await client.ticket.fetch_tickets(ticket_payload,1)
            ticket_messages = await client.ticket.fetch_ticket_message(tickets, 1)
            file_name = os.path.join('csv', "ticket_messages.csv")
            ticket_messages.to_csv(file_name, index=False)
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