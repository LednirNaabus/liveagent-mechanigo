# Mechanigo and LiveAgent Data Pipeline

A streamlined data extraction solution to extract conversation data from LiveAgent API for chat analysis using FastAPI and OpenAI.

## Local use
- To run locally, install the dependencies first:
```
pip install -r requirements.txt
```

- Configure your secrets such as OpenAI, LiveAgent and Google API keys.

- Finally, run the **FastAPI** server using:
```
fastapi dev app.py
```

# Documentation
- You can access the docs by going to `localhost:8000/docs`.

## Endpoints
### **POST**

1. `/update-agents/{table_name}`
2. `/update-users/{table_name}`
3. `/update-tags/{table_name}`
4. `/update-tickets/{table_name}`
    - **Parameters**:
        - `table_name (str)`
            - Name of table you want the data to be saved on.
        - `is_initial (boolean)`
            - Used if extracting inital dates (to be used with the `date` parameter)
        - `date (str)`
            - Date you want to extract data from. Format: `YYYY-MM-DD`. (**Important**: Date should be start of month, i.e. `2025-01-01`, or `2025-04-01`)
5. `/update-ticket-messages/{table_name}`
    - **Parameters**:
        - `table_name (str)`
            - Name of table you want the data to be saved on.
        - `is_initial (boolean)`
            - Used if extracting inital dates (to be used with the `date` parameter)
        - `date (str)`
            - Date you want to extract data from. Format: `YYYY-MM-DD`. (**Important**: Date should be start of month, i.e. `2025-01-01`, or `2025-04-01`)
6. `/update-chat-analysis/{table_name}`
    - **Parameters**:
        - `table_name (str)`
            - Name of table you want the data to be saved on.
7. `/extract-logs/{table_name}`