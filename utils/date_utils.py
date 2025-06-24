import json
import pandas as pd
from enum import Enum

class FilterField(str, Enum):
    DATE_CREATED = "date_created"
    DATE_CHANGED = "date_changed"

def set_filter(date: pd.Timestamp, filter_field: FilterField = FilterField.DATE_CREATED):
    """
    Returns a JSON-encoded filter for a 6-hour time window based on the specified field.
    
    - If `filter_field` is `DATE_CREATED`: filter for the whole month of the date provided
    - If `filter_field` is `DATE_CHANGED`: filter for a 6-hour window ending at the current timestamp

    Parameters:
        date (`pd.Timestamp`): Reference datetime.
        filter_field (`FilterField`): Enum that is either `DATE_CREATED` or `DATE_CHANGED`.

    Returns:
        str: JSON-encoded filter.
    """
    if filter_field == FilterField.DATE_CREATED:
        start = date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        end = (start + pd.offsets.MonthEnd(1)).replace(hour=23, minute=59, second=59)
    else:
        start = date.floor('h')
        end = start + pd.Timedelta(hours=6) - pd.Timedelta(seconds=1)
    return json.dumps([
        [filter_field.value, "D>=", f"{start}"],
        [filter_field.value, "D<=", f"{end}"]
    ])

def set_timezone(df: pd.DataFrame, *columns: str, target_tz: str) -> pd.DataFrame:
    """
    """
    for column in columns:
        df[column] = pd.to_datetime(df[column], errors="coerce")
        if df[column].dt.tz is None:
            df[column] = df[column].dt.tz_localize("UTC")
        else:
            df[column] = df[column].dt.tz_convert("UTC")
        df[column] = df[column].dt.tz_convert(target_tz).dt.tz_localize(None)
    return df

def format_date_col(df: pd.DataFrame, *columns: str, format: str = "%Y-%m-%d %H:%M:%S") -> pd.DataFrame:
    """
    """
    for column in columns:
        df[column] = df[column].dt.strftime(format)
    return df