import asyncio
import aiohttp
import logging
import traceback
import pandas as pd
from tqdm import tqdm
from typing import Optional
from google.cloud import bigquery
from utils.bq_utils import get_client, sql_query_bq

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

class LiveAgentClient:
    def __init__(self, api_key: str, project_id: str = None, dataset_id: str = None):
        self.BASE_URL = "https://mechanigo.ladesk.com/api/v3"
        self.api_key = api_key
        self.sem = asyncio.Semaphore(2)
        self.THROTTLE_DELAY = 0.4
        self.session: Optional[aiohttp.ClientSession] = None
        self.agent: Optional['LiveAgentClient.Agent'] = None
        self.ticket: Optional['LiveAgentClient.Ticket'] = None
        
        # BigQuery integration
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.bq_config = get_client() if project_id else None
        self.bq_client = self.bq_config['client'] if self.bq_config else None

        # cache for agents data
        self.agents_cache = {}
        self.unique_userids = set()

    def default_headers(self):
        return {
            'accept': 'application/json',
            'apikey': self.api_key
        }

    async def __aenter__(self):
        await self.start_session()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            logging.error(f"Exception in LiveAgentClient context: {exc_type.__name__}: {exc_val}")
        await self.close_session()
        return False

    async def start_session(self):
        if self.session is None:
            self.session = aiohttp.ClientSession()
            self.agent = self.Agent(self.BASE_URL, self.api_key, self.session, self.default_headers())
            self.ticket = self.Ticket(self.BASE_URL, self.api_key, self.session, self.default_headers(), self)
    
    async def close_session(self):
        if self.session:
            await self.session.close()
            self.session = None
            self.agent = None
            self.ticket = None

    async def ping(self) -> tuple[bool, dict]:
        if self.session is None:
            await self.start_session()
        try:
            async with self.session.get(f"{self.BASE_URL}/ping", headers=self.default_headers()) as response:
                status_ok = response.status == 200
                try:
                    response_json = await response.json()
                except aiohttp.ContentTypeError:
                    response_json = {"message": "Non-JSON response"}
                return status_ok, response_json
        except aiohttp.ClientError as e:
            logging.error(f"'LiveAgentClient.ping()': {e}")
            traceback.print_exc()
            return False, {}
        except Exception as e:
            logging.error(f"Unexpected error in 'ping()': {e}")
            traceback.print_exc()
            return False, {"error": str(e)}

    async def paginate(self, url: str, payload: dict, headers: dict, max_pages: int) -> list:
        if self.session is None:
            await self.start_session()
        all_data = []
        page = 1

        while page <= max_pages:
            payload["_page"] = page
            async with self.sem:
                await asyncio.sleep(self.THROTTLE_DELAY)
                try:
                    async with self.session.get(url=url, params=payload, headers=headers) as response:
                        response.raise_for_status()
                        data = await response.json()

                    if isinstance(data, dict):
                        data = data.get('data', [])

                    if not data:
                        break

                    all_data.extend(data)
                    page += 1
                except aiohttp.ClientError as e:
                    logging.error(f"Error during pagination on page {page}: {e}")
                    break
        return all_data

    async def get_user(self, user_id: str) -> pd.DataFrame:
        if self.session is None:
            await self.start_session()

        try:
            async with self.session.get(
                f"{self.BASE_URL}/users/{user_id}",
                headers=self.default_headers()
            ) as response:
                response.raise_for_status()
                data = await response.json()
            return pd.DataFrame(data)
        except aiohttp.ClientError as e:
            logging.error(f"Error getting user {user_id}: {e}")
            raise

    async def fetch_tags(self) -> pd.DataFrame:
        if self.session is None:
            await self.start_session()

        try:
            async with self.session.get(
                f"{self.BASE_URL}/tags",
                headers=self.default_headers()
            ) as response:
                response.raise_for_status()
                data = await response.json()
            return pd.DataFrame(data)
        except aiohttp.ClientError as e:
            logging.error(f"Error getting tags: {e}")
            raise

    async def load_agents_cache(self):
        if self.bq_client and self.dataset_id:
            try:
                query = f"""
                SELECT id, name
                FROM `{self.dataset_id}.agents`
                """
                result = sql_query_bq(query)
                self.agents_cache = dict(zip(result['id'], result['name']))
                logging.info(f"Loaded {len(self.agents_cache)} agents from BigQuery.")
            except Exception as e:
                logging.warning(f"Could not load agents from BigQuery: {e}. Trying loading from LiveAgent API...")
        try:
            agents_data = await self.agent.get_agents(max_pages=10)
            for agent in agents_data:
                self.agents_cache[agent['id']] = agent.get('name', agent.get('name', 'Unknown'))
            logging.info(f"Loaded {len(self.agents_cache)} agents from '{self.BASE_URL}/agents'")
        except Exception as e:
            logging.error(f"Failed to load agents from '{self.BASE_URL}/agents': {e}")
            self.agents_cache = {}

    def determine_sender_receiver(self, userid: str, owner_name: str, agentid: str) -> tuple[str, str, str, str]:
        if userid == "system00":
            return "system", "system", owner_name, "client"

        if userid in self.agents_cache:
            agent_name = self.agents_cache[userid]
            return agent_name, "agent", owner_name, "client"
        else:
            agent_name = self.agents_cache.get(agentid, "Unkown Agent")
            return owner_name, "client", agent_name, "agent"

    def define_messages_schema(self):
        return [
            bigquery.SchemaField("ticket_id", "STRING"),
            bigquery.SchemaField("owner_name", "STRING"),
            bigquery.SchemaField("agent_name", "STRING"),
            bigquery.SchemaField("id", "STRING"),
            bigquery.SchemaField("parent_id", "STRING"),
            bigquery.SchemaField("userid", "STRING"),
            bigquery.SchemaField("user_full_name", "STRING"),
            bigquery.SchemaField("type", "STRING"),
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("datecreated", "DATETIME"),
            bigquery.SchemaField("datefinished", "DATETIME"),
            bigquery.SchemaField("sort_order", "INTEGER"),
            bigquery.SchemaField("mail_msg_id", "STRING"),
            bigquery.SchemaField("pop3_msg_id", "STRING"),
            bigquery.SchemaField("message_id", "STRING"),
            bigquery.SchemaField("message_userid", "STRING"),
            bigquery.SchemaField("message_type", "STRING"),
            bigquery.SchemaField("message_datecreated", "DATETIME"),
            bigquery.SchemaField("message_format", "STRING"),
            bigquery.SchemaField("message", "STRING"),
            bigquery.SchemaField("message_visibility", "STRING")
        ]

    def batch_insert_to_bq(self, table_name: str, data: list[dict], schema: list = None, write_mode: str = "WRITE_APPEND"):
        if not self.bq_client or not self.dataset_id or not data:
            return

        table_ref = self.bq_client.dataset(self.dataset_id).table(table_name)

        job_config = bigquery.LoadJobConfig(
            write_disposition=write_mode
        )

        if schema:
            job_config.schema = schema
        else:
            job_config.autodetect = True

        job = self.bq_client.load_table_from_json(data, table_ref, job_config=job_config)
        job.result()

        logging.info(f"Inserted {len(data)} records to {table_name}")
    
    async def populate_users_from_collected_ids(self, batch_size: int = 50):
        if not self.unique_userids:
            logging.warning("No user IDs collected.")
            return

        users_data = []

        for userid in tqdm(self.unique_userids, desc="Fetching users"):
            try:
                if userid == "system00":
                    system_user = {
                        'id': 'system00',
                        'role': 'system'
                    }
                    users_data.append(system_user)
                else:
                    user_df = await self.get_user(userid)
                    if not user_df.empty:
                        users_data.append(user_df.iloc[0].to_dict())

                if len(users_data) >= batch_size:
                    self.batch_insert_to_bq('users', users_data, write_mode="WRITE_APPEND")
                    users_data = []
            except Exception as e:
                logging.error(f"Error fetching user {userid}: {e}")
                continue

        if users_data:
            self.batch_insert_to_bq('users', users_data, write_mode="WRITE_APPEND")

    class _BaseResource:
        """
        Base class for all LiveAgent API endpoints/resources.
        """
        def __init__(self, base_url: str, api_key: str, session: aiohttp.ClientSession, headers: dict = None, parent_client: Optional['LiveAgentClient'] = None):
            self.base_url = base_url
            self.api_key = api_key
            self.session = session
            self.headers = headers or {}
            self.parent_client = parent_client

        def _get_temp_client(self):
            """
            Create temporary client that shares session for pagination purposes.
            """
            temp_client = LiveAgentClient.__new__(LiveAgentClient)
            temp_client.session = self.session
            temp_client.sem = asyncio.Semaphore(2)
            temp_client.THROTTLE_DELAY = 0.4
            return temp_client

    class Agent(_BaseResource):
        async def get_agents(self, max_pages: int = 5) -> dict:
            url = f"{self.base_url}/agents"
            payload = {
                "_page": 1
            }

            temp_client = self._get_temp_client()
            return await temp_client.paginate(
                url=url,
                payload=payload,
                headers=self.headers,
                max_pages=max_pages
            )

    class Ticket(_BaseResource):
        async def fetch_tickets(self, payload: dict, max_pages: int = 5) -> pd.DataFrame:
            url = f"{self.base_url}/tickets"

            temp_client = self._get_temp_client()
            try:
                ticket_data = await temp_client.paginate(
                    url=url,
                    payload=payload,
                    headers=self.headers,
                    max_pages=max_pages
                )
                for ticket in tqdm(ticket_data, desc='Fetching tickets'):
                    ticket['tags'] = ','.join(ticket['tags']) if ticket.get('tags') else ''
                    ticket['date_due'] = ticket.get('date_due')
                    ticket['date_deleted'] = ticket.get('date_deleted')
                    ticket['date_resolved'] = ticket.get('date_resolved')

                return pd.DataFrame(ticket_data)
            except Exception as e:
                logging.error(f"Exception occured in 'fetch_tickets()': {e}")
                traceback.print_exc()

        async def fetch_ticket_message(self, ticket_payload: dict, max_pages: int = 5, message_per_page: int = 5, insert_to_bq: bool = False, batch_size: int = 500) -> pd.DataFrame:
            # logging.debug(f"parent_client: {self.parent_client}")
            # logging.debug(f"type of parent_client: {type(self.parent_client)}")
            if not self.parent_client.agents_cache:
                await self.parent_client.load_agents_cache()

            ticket_ids = ticket_payload.get("id", [])
            ticket_owner_names = ticket_payload.get("owner_name", [])
            ticket_agent_ids = ticket_payload.get("agentid", [])

            processed_messages = []

            message_payload = {
                "page": 1,
                "_perPage": message_per_page
            }

            temp_client = self._get_temp_client()

            try:
                for ticket_id, ticket_owner_name, ticket_agent_id in tqdm(
                    zip(ticket_ids, ticket_owner_names, ticket_agent_ids),
                    total=len(ticket_ids),
                    desc="Processing ticket messages"
                ):
                    ticket_messages_url = f"{self.base_url}/tickets/{ticket_id}/messages"
                    ticket_messages_payload = message_payload.copy()

                    data = await temp_client.paginate(
                        url=ticket_messages_url,
                        payload=ticket_messages_payload,
                        headers=self.headers,
                        max_pages=max_pages
                    )

                    # get agent name for ticket
                    agent_name = self.parent_client.agents_cache.get(ticket_agent_id, "Unknown Agent")
                    for message_group in data:
                        message_group_data = {
                            'ticket_id': ticket_id,
                            'owner_name': ticket_owner_name,
                            'agentid': ticket_agent_id,
                            'agent_name': agent_name,
                            'id': message_group.get('id'),
                            'parent_id': message_group.get('parent_id'),
                            'userid': message_group.get('userid'),
                            'user_full_name': message_group.get('user_full_name'),
                            'type': message_group.get('type'),
                            'status': message_group.get('status'),
                            'datecreated': message_group.get('datecreated'),
                            'datefinished': message_group.get('datefinished'),
                            'sort_order': message_group.get('sort_order'),
                            'mail_msg_id': message_group.get('mail_msg_id'),
                            'pop3_msg_id': message_group.get('pop3_msg_id'),
                            'message_id': None,
                            'message_userid': None,
                            'message_type': None,
                            'message_datecreated': None,
                            'message_format': None,
                            'message': None,
                            'message_visibility': None
                        }

                        sender_name, sender_type, receiver_name, receiver_type = self.parent_client.determine_sender_receiver(
                            message_group.get('userid'), ticket_owner_name, ticket_agent_id
                        )

                        message_group_data.update({
                            'sender_name': sender_name,
                            'sender_type': sender_type,
                            'receiver_name': receiver_name,
                            'receiver_type': receiver_type
                        })

                        self.parent_client.unique_userids.add(message_group.get('userid'))

                        if 'messages' in message_group and message_group['messages']:
                            for nested_msg in message_group['messages']:
                                nested_msg_data = message_group_data.copy()
                                nested_msg_data.update({
                                    'message_id': nested_msg.get('id'),
                                    'message_userid': nested_msg.get('userid'),
                                    'message_type': nested_msg.get('type'),
                                    'message_datecreated': nested_msg.get('datecreated'),
                                    'message_format': nested_msg.get('format'),
                                    'message': nested_msg.get('message'),
                                    'message_visibility': nested_msg.get('visibility')
                                })

                                sender_name, sender_type, receiver_name, receiver_type = self.parent_client.determine_sender_receiver(
                                    nested_msg.get('userid'), ticket_owner_name, ticket_agent_id
                                )

                                nested_msg_data.update({
                                    'sender_name': sender_name,
                                    'sender_type': sender_type,
                                    'receiver_name': receiver_name,
                                    'receiver_type': receiver_type
                                })

                                self.parent_client.unique_userids.add(nested_msg.get('userid'))
                                processed_messages.append(nested_msg_data)
                        else:
                            processed_messages.append(message_group_data)

                    if insert_to_bq and len(processed_messages) >= batch_size:
                        self.parent_client.batch_insert_to_bq(
                            'messages_test',
                            processed_messages,
                            schema=self.parent_client.define_messages_schema()
                        )
                        processed_messages = []

                if insert_to_bq and processed_messages:
                    self.parent_client.batch_insert_to_bq(
                        'messages_test',
                        processed_messages,
                        schema=self.parent_client.define_messages_schema()
                    )
                    processed_messages = []
            except Exception as e:
                logging.error(f"Error fetching ticket messages: {e}")
                traceback.print_exc()

            return pd.DataFrame(processed_messages)

        def get_convo_str(self):
            """
            Get messages from messages table and convert them to string.
            """
            query = f"""
            SELECT *
            FROM `mechanigo-liveagent.conversations.messages`
            """
            df_messages = sql_query_bq(query)
            s = [f'datecreated:{m["datecreated"]}\nrole: {m["role"]}\nmessage: "{m["message"]}"' for m in df_messages]
            # logging.info(f"{__name__}")
            return s