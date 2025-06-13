import pandas as pd

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