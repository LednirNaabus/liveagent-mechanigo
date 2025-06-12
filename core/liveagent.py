import asyncio
import aiohttp
import logging
import traceback
import pandas as pd
from tqdm import tqdm
from typing import Optional

logging.basicConfig(format = '%(asctime)s-%(levelname)s-%(message)s', datefmt='%Y-%m-%d %H:%M%:S', level=logging.INFO)

class LiveAgentClient:
    def __init__(self, api_key: str):
        self.BASE_URL = "https://mechanigo.ladesk.com/api/v3"
        self.api_key = api_key
        self.sem = asyncio.Semaphore(2)
        self.THROTTLE_DELAY = 0.4
        self.session: Optional[aiohttp.ClientSession] = None
        self.agent: Optional['LiveAgentClient.Agent'] = None
        self.ticket: Optional['LiveAgentClient.Ticket'] = None

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
            self.ticket = self.Ticket(self.BASE_URL, self.api_key, self.session, self.default_headers())
    
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

    class _BaseResource:
        """
        Base class for all LiveAgent API endpoints/resources.
        """
        def __init__(self, base_url: str, api_key: str, session: aiohttp.ClientSession, headers: dict = None):
            self.base_url = base_url
            self.api_key = api_key
            self.session = session
            self.headers = headers or {}

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

                return pd.DataFrame(ticket_data)
            except Exception as e:
                logging.error(f"Exception occured in 'fetch_tickets()': {e}")
                traceback.print_exc()