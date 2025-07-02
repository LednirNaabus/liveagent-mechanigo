import logging
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

def fill_nan_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fills `NaN` values in a DataFrame with appropriate values based on the column type.

    - String columns will have `NaN` filled with an empty string `""`.
    
    - Numeric columns will have `NaN` filled with zero (`0`).
    """
    for col in df.columns:
        if df[col].dtype == "object":
            df[col].fillna("", inplace=True)
        else:
            df[col].fillna(0, inplace=True)
    return df

def drop_cols(df: pd.DataFrame, *cols: str) -> pd.DataFrame:
    """
    Accepts a `pandas` DataFrame, and iteratively drops each column provided.
    """
    try:
        existing = [col for col in cols if col in df.columns]
        if existing:
            df.drop(columns=existing, inplace=True)
    except Exception as e:
        logging.warning(f"Current columns: {df.columns.tolist()}")
        logging.error(f"Exception occured while dropping columns: {e}")
    return df