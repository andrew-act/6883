import pandas as pd
import numpy as np
from datetime import datetime
import os

def clean_data(df):
    # cenvert timestamp to datetime
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s', errors='coerce')

    # fill the missing value
    df.ffill(inplace=True)
    return df


def normalize_uniswap_df(df: pd.DataFrame) -> pd.DataFrame:
    """
   Perform field identification and intelligent normalization on Uniswap data.
    Choose log, Min-Max or Z-score normalization based on the keywords contained in the field names.

    Parameters: 
 df (pd.DataFrame): raw data table

    Returns: 
 pd.DataFrame: contains only normalized fields
    """

# Define the normalization function
    def min_max_normalize(series):
        return (series - series.min()) / (series.max() - series.min()) if series.max() != series.min() else np.zeros_like(series)

    def z_score_normalize(series):
        return (series - series.mean()) / series.std() if series.std() != 0 else np.zeros_like(series)

    def log_normalize(series):
        return np.log1p(series)  # 避免 log(0)

# Define the mapping of keywords to normalization methods
    normalization_rules = {
        'usd': log_normalize,
        'amount': log_normalize,
        'volume': log_normalize,
        'reserve': log_normalize,
        'liquidity': log_normalize,
        'price': z_score_normalize,
        'txcount': min_max_normalize,
        'dailytxns': min_max_normalize,
    }

    # Storing normalized columns
    normalized_data = {}

    #  Get all value columns

    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()

    for col in numeric_cols:
        col_lower = col.lower()
        for keyword, func in normalization_rules.items():
            if keyword in col_lower:
                try:
                    df[col] = func(df[col])  # Direct replacement of the original column
                    break
                except Exception as e:
                    print(f"Error while normalizing field {col}：{e}")
    return df


excel_files = [
    'swaps_data.xlsx',
    'burns_data.xlsx',
    'mints_data.xlsx',
    'uniswapDayDatas_data.xlsx',
    'pairDayDatas_data.xlsx',
    'tokenDayDatas.xlsx'
]

for file in excel_files:
    # read Excel file
    df = pd.read_excel(file)
    # Cleaning and standardization of data
    df_1 = clean_data(df)
    df_cleaned =  normalize_uniswap_df(df)
    # construct new file name
    csv_filename = 'normalized_' + file.replace('.xlsx', '.csv')
   # save as CSV ，remove index
    df_cleaned.to_csv(csv_filename, index=False)
    print(f"sava as {csv_filename}")

