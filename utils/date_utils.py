import json
import pandas as pd

def set_filter(date: pd.Timestamp):
    """
    """
    start = date.floor('h')
    end = start + pd.Timedelta(hours=6) - pd.Timedelta(seconds=1)
    return json.dumps([
        ["date_created", "D>=", f"{start}"],
        ["date_created", "D<=", f"{end}"]
    ])

def set_timezone(df: pd.DataFrame, *columns: str, target_tz: str) -> pd.DataFrame:
    """
    """
    for column in columns:
        df[column] = pd.to_datetime(df[column], errors="coerce").dt.tz_localize('UTC')
        df[column] = df[column].apply(
            lambda x: x.astimezone(target_tz).replace(tzinfo=None) if pd.notnull(x) else x
        )
    return df

def format_date_col(df: pd.DataFrame, *columns: str, format: str = "%Y-%m-%d") -> pd.DataFrame:
    """
    """
    for column in columns:
        df[column] = df[column].dt.strftime(format)
    return df