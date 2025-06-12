import json
import asyncio
import aiohttp
import pandas as pd
from config import config
from core.liveagent import LiveAgentClient
from utils.bq_utils import generate_schema, load_data_to_bq

async def main():
    async with LiveAgentClient(config.API_KEY) as client:
        success, ping_response = await client.ping()
        if success:
            print(f"Ping to {client.BASE_URL} successful.")
            user = await client.get_user("00054iwg")
            print(user)
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