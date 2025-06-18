import json
import logging
import tiktoken
from typing import Dict
from pydantic import BaseModel
from openai import OpenAI, AuthenticationError, OpenAIError
from core.liveagent import LiveAgentClient
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
        purpose: str  
        summary : str
        intent_rating : str
        engagement_rating : int
        clarity_rating : int
        resolution_rating : int
        sentiment_rating: str
        location: str
        schedule_date: str
        schedule_time: str
        car : str
        inspection: str
        quotation: str
        service_category: str

    def __init__(
            self,
            ticket: LiveAgentClient.Ticket = None,
            api_key: str = None,
            temperature: int = 0.8
    ):
        self.client = self.create_client(api_key=api_key)
        self.model = 'gpt-4o'
        self.temperature = temperature
        self.ticket = ticket
        if self.ticket:
            self.conversation_text = self.ticket.get_convo_str()
        self.prompt = f"""
            You are a conversation analyst for Mechanigo.ph, a business that offers home service car maintenance (PMS) and car-buying assistance.

            Your task: 
            - Analyze the following conversation between a customer and a service agent. 
            - The conversation will include a mix of english and filipino.
            - Assess the conversation based on how well it supports the customer's goals and Mechanigo's service quality.
            - Extract or determine the necessary information from the conversation.
            
            Chat:
            {self.conversation_text}
            
            Use the following rubrics for each score:
            
            1. **Service**
                - type: str
                - description: type of service inquired or discussed. 
                - examples:
                  - Preventive Maintenance Services (PMS)
                  - Car Buying Assistance
                  - Diagnosis
                  - Parts Replacement  
            
            2. **Summary**  
               - Provide a brief, 1-2 sentence overview of what the customer wanted and what the agent responded with.
            
            3. **Intent Rating** (No Intent, Low Intent, Moderate Intent, High Intent):  
               - No Intent: General greeting or message not related to any service.  
               - Low Intent: Vague question or comment with no clear ask (e.g., "How does this work?").  
               - Moderate Intent: Specific inquiry about a service or product (e.g., “How much is a PMS for Toyota Vios?”).  
               - High Intent: Ready to book or buy, asking about schedules, availability, or actively deciding (e.g., “Can I book this Friday at 10AM?”).
            
            4. **Engagement Rating** (Low, Medium, High):  
               - Low: One-sided conversation, short replies, or customer drops off early.  
               - Medium: Some back-and-forth but not deeply interactive.  
               - High: Multiple exchanges, customer asks follow-up questions, actively involved.
            
            5. **Sentiment Rating** (Negative, Neutral, Positive):  
               - Negative: Complaints, frustration, sarcasm.  
               - Neutral: Information seeking without emotional tone.  
               - Positive: Politeness, satisfaction, appreciation, excitement.
            
            6. **Resolution Rating** (1 to 10):  
               - 1-3: Agent response did not help at all or was irrelevant.  
               - 4-6: Agent partially addressed the issue, but left key questions unanswered.  
               - 7-8: Issue mostly resolved but with minor gaps (e.g., unclear price, scheduling details).  
               - 9-10: Fully resolved, clear answers, and next steps or confirmation provided.
            
            7. **Clarity Rating** (1 to 10):  
               - 1-3: Agent used vague or confusing language, technical jargon, or incorrect info.  
               - 4-6: Some helpful information, but phrasing or tone might confuse customers.  
               - 7-8: Mostly clear, minor lapses in tone, flow, or terminology.  
               - 9-10: Very easy to understand, concise, on-brand, and professional tone.

            8. **Location**
                - type: str
                - description: Client's address or location prefaced by the following agent spiels:
                    - Could you please let me know the location where you plan to purchase the vehicle?
                    - Could you please let me know your exact location, so we can check if it's within our serviceable area po
                    - Could you please let me know your address?
                    - May I know where you're located po?
                    - Saan po kayo nakatira?
                    - San po kayo nakatira?
                - examples:
                    - Sample St., 123 Building, Brgy. Olympia, Makati City
                    - 1166 Chino Roces Avenue, Corner Estrella St, Makati City

            9. **Schedule Date**
                - type: str
                - description: client's appointment schedule date. Infer the date from the conversation and output in this format: YYYY-MM-DD
                    Adjust accordingly to date modifiers like tomorrow, bukas, etc.
                - examples:
                    - 2025-01-01 
                    - Jan 1, 2025
                    - March 31
            
            10. **Schedule Time**
                - type: str
                - description: client's appointment schedule time. Infer the time from the conversation and output in this format: HH:MM AM/PM
                - examples:
                    - 11AM
                    - 3PM

            11. **Car**
                - type: str
                - description: client's car information including the car brand, car model, car year,
                and possibly the variety or trim.
                - examples:
                    - Toyota Vios 2021
                    - 2020 Honda Civic A/T
                    - 2023 Mitsubishi Outlander SE
                    
            12. **Inspection**
                - type :Str
                - description: car inspection results as described by the agent. This involves
                cracks, defects, car issues, etc with potential recommendations
            
            13. **Quotation**
                - type: str
                - description: quotation based from the recommendations sent as described by the agent which
                may include parts replacement prices, service costs, and fees.
                
            - If not mentioned, leave the corresponding field blank.
            - Make sure the location mentioned is located in the Philippines only.
        """

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