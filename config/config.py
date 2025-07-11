import os
import json
import pytz
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

load_dotenv()

MNL_TZ = pytz.timezone('Asia/Manila')
API_KEY = os.getenv("API_KEY")
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
CREDS_FILE = json.loads(os.getenv('CREDENTIALS'))
SCOPE = [
    'https://www.googleapis.com/auth/bigquery'
]
GOOGLE_CREDS = service_account.Credentials.from_service_account_info(CREDS_FILE, scopes=SCOPE)
BQ_CLIENT = bigquery.Client(credentials=GOOGLE_CREDS, project=GOOGLE_CREDS.project_id)

GOOGLE_PROJ_INFO = {
    "BIGQUERY": {
        "project_id": "mechanigo-liveagent",
        "dataset_name": "conversations"
    }
}

GCLOUD_PROJECT_ID = GOOGLE_PROJ_INFO.get("BIGQUERY")['project_id']
BQ_DATASET_NAME = GOOGLE_PROJ_INFO.get("BIGQUERY")['dataset_name']

# For ConvoDataExtract
PROMPT="""
You are a conversation analyst for Mechanigo.ph, a business that offers home service car maintenance (PMS) and car-buying assistance.

Your task: 
- Analyze the following conversation between a customer and a service agent. 
- The conversation will include a mix of english and filipino.
- Assess the conversation based on how well it supports the customer's goals and Mechanigo's service quality.
- Extract or determine the necessary information from the conversation.

Chat:
{conversation_text}

Use the following rubrics for each score:

1. **Service Category**
    - type: str
    - description: type of service inquired or discussed. 
    - options:
        - Preventive Maintenance Services (PMS)
        - Car Buying Assistance
        - Diagnosis
        - Parts Replacement  

2. **Summary**  
    - Provide a brief, 1-2 sentence overview of what the customer wanted and what the agent responded with.

3. **Intent Rating**:  
    - Reflects customer's interest level based on shared details and next steps.
    - type: str
    - options:
        - No Intent, Low Intent, Moderate Intent, Hight Intent, Hot Intent
    - Guidelines:
        - **No Intent**:
            - Customer never replies after 
        - **Low Intent**:
            - Customer only asks general inquiries such as:
                - Price list
                - Services offered
                - Service areas
                - Referral questions
        - **Moderate Intent**:
            - Customer provides at least 2 of the following:
                - Vehicle details (brand, model, year, etc.)
                - Service needed (from Service Category)
                - Address or location
                - Their contact number
                - Tire brand, size, or quantity
        - **High Intent**:
            - For **non-Car Buying Assistance**:
                - Must include *all* of the following:
                    - Service needed
                    - Vehicle details
                    - Address or location
                    - Contact number
                    - Tire details
                - AND one of the following:
                    - Shared their available schedule
                    - Asked about the your available schedule
            - For **Car Buying Assistance**:
                - Mentioning interest in a schedule (e.g., "When can we schedule?", "Pwede ba bukas?") is enough
        - **Hot Intent**:
            - Customer explicitly books a service OR confirms payment

4. **Engagement Rating**:  
    - How interactive the conversation is.
    - type: int
    - Options: 1 to 10
        - 1-3: One-sided conversation, short replies, or customer drops off early.  
        - 4-6: Some back-and-forth but not deeply interactive.  
        - 7-10: Ongoing exchange, customer asks follow-up questions, actively involved.

5. **Sentiment Rating** (Negative, Neutral, Positive):  
    - Negative: Complaints, frustration, sarcasm.  
    - Neutral: Information seeking without emotional tone.  
    - Positive: Politeness, satisfaction, appreciation, excitement.

6. **Resolution Rating** (1 to 10):  
    - type: str
    - How well the agent resolved the inquiry
    - 1-3: Agent response did not help at all or was irrelevant.  
    - 4-6: Agent partially addressed the issue, but left key questions unanswered.  
    - 7-8: Issue mostly resolved but with minor gaps (e.g., unclear price, scheduling details).  
    - 9-10: Fully resolved, clear answers, and next steps or confirmation provided.

7. **Clarity Rating** (1 to 10):  
    - How clearly the agent communicated
    - type: str
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
    - Note that sometimes their location details is provided using a template like this:
        Name:
        Contact Number:
        Exact Address (with Barangay):
        - In this case extract only the "Exact Address (with Barangay)"
    - examples:
        - Sample St., 123 Building, Brgy. Olympia, Makati City
        - 1166 Chino Roces Avenue, Corner Estrella St, Makati City
        - Quezon City
        - Taguig
        - Cavite

9. **Schedule Date**
    - type: str
    - description: client's appointment schedule date. Infer from context (e.g., "bukas" -> tomorrow)
    - format: YYYY-MM-DD
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
    - description:
        - client's car information including:
            - vehicle details:
                - car brand
                - car model
                - car year
                - variety or trim
            - tire details:
                - tire brand
                - tire size
                - tire quantity
    - examples:
        - Toyota Vios 2021
        - 2020 Honda Civic A/T
        - 2023 Mitsubishi Outlander SE
        - Toyota Supra
        - Fronway 165/65/R13
        - Michelin 175/65/R14 4 pcs.

12. **Contact Num**
    - type: str
    - description: the customer or client's provided contact number details. Note that sometimes their contact details is provided using a template like this:
        Name:
        Contact Number:
        Exact Address (with Barangay):
        - In this case extract only the "Contact Number"
    - examples:
        - 0967123456
        - Contact number: 0965123456

13. **Payment**
    - type: str
    - description:
        - payment amount
            - examples:
                - Php 5,000
                - 15000
                - 10000 pesos
                - 213123.89
        - payment method
            - examples:
                - cash
                - Gcash
                - Bank Transfer
                - Credit Card
        
14. **Inspection**
    - type :Str
    - description: car inspection results as described by the agent. This involves
    cracks, defects, car issues, etc with potential recommendations

15. **Quotation**
    - type: str
    - description: quotation based from the recommendations sent as described by the agent which
    may include parts replacement prices, service costs, and fees.

16. **Model**
    - type: str
    - description: The GPT model used for the analysis (default is gpt-4.1-mini)

**Important Notes:**
- If not mentioned, leave the corresponding field blank.
- Make sure the location mentioned is located in the Philippines only.
"""