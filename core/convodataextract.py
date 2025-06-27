import json
import logging
import tiktoken
from typing import Dict
from pydantic import BaseModel
from openai import OpenAI, AuthenticationError, OpenAIError
from utils.bq_utils import sql_query_bq
from config import config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

def count_tokens(text: str) -> int:
    try:
        encoder = tiktoken.encoding_for_model("gpt-4o-mini")
        num_tokens = len(encoder.encode(text))
    except Exception as e:
        logging.error(f"Exception occurred in 'count_tokens()': {e}")
        logging.error("'num_tokens' is set to 0.")
        num_tokens = 0
    return num_tokens

class ConvoDataExtract:
    class ResponseSchema(BaseModel):
        service_category: str  
        summary : str
        intent_rating : str
        engagement_rating : int
        clarity_rating : int
        resolution_rating : int
        sentiment_rating: str
        location: str
        schedule_date: str
        schedule_time: str
        car: str
        inspection: str
        quotation: str

    def __init__(
            self,
            ticket_id: str = None,
            api_key: str = None,
            temperature: int = 0.8
    ):
        self.client = self.create_client(api_key=api_key)
        self.model = 'gpt-4o'
        self.temperature = temperature
        self.ticket_id = ticket_id
        if self.ticket_id:
            self.conversation_text = self.get_convo_str(ticket_id)
        self.prompt = config.PROMPT.format(conversation_text=self.conversation_text)
        if self.conversation_text:
            self.data = self.analyze_convo()

    def create_client(self, api_key: str = None) -> OpenAI:
        key_sources = [
            ("provided", api_key),
            ("env", config.OPENAI_API_KEY)
        ]
        for source, key in key_sources:
            if not key:
                continue
            try:
                client = OpenAI(api_key=key)
                client.models.list()
                logging.info(f"OpenAI client initialized using {source} key.")
                return client
            except (AuthenticationError, OpenAIError) as e:
                logging.error(f"Failed with {source} key: {e}")
                continue
    
    def analyze_convo(self) -> Dict:
        if not self.prompt:
            raise Exception('Prompt not specified.')

        messages = [
            {
                "role": "system",
                "content": self.prompt
            }
        ]

        try:
            response = self.client.beta.chat.completions.parse(
                model=self.model,
                messages=messages,
                response_format=self.ResponseSchema
            )

            return {
                'data': json.loads(response.choices[0].message.content),
                'tokens': response.usage.total_tokens
            }
        except Exception as e:
            output = {
                'data': {
                    "service_category": None,
                    "car": None,
                    "location": None,
                    "summary": None,
                    "intent_rating": None,
                    "engagement_rating": None,
                    "clarity_rating": None,
                    "resolution_rating": None,
                    "sentiment_rating": None
                },
                'tokens': count_tokens(self.prompt)
            }

            return output

    def get_convo_str(self, ticket_id: str) -> str:
        """
        Get messages from messages table and convert them to string.
        """
        query = f"""
        SELECT datecreated, sender_type, message
        FROM `mechanigo-liveagent.conversations.messages`
        WHERE ticket_id = '{ticket_id}'
        ORDER BY datecreated
        """
        df_messages = sql_query_bq(query)
        s = [
            f'datecreated:{m[["datecreated"]]}\nsender: {m["sender_type"]}\nmessage: {m["message"]}'
            for _, m in df_messages.iterrows()
        ]
        return "\n\n".join(s)