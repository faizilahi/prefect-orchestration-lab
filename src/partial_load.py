import pandas as pd

def buggy_retry_load(src: pd.DataFrame) -> pd.DataFrame:
    attempt1 = src.iloc[:6000]
    attempt2 = src  # full replay without dedupe
    return pd.concat([attempt1, attempt2], ignore_index=True)

def clean_load(src: pd.DataFrame) -> pd.DataFrame:
    return src.copy()
