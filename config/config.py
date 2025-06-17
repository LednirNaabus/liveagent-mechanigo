import os
import json
import pytz
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

load_dotenv()

MNL_TZ = pytz.timezone('Asia/Manila')
API_KEY = os.getenv("API_KEY")
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