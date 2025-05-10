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
    对 Uniswap 数据进行字段识别与智能归一化处理。
    根据字段名包含的关键词，选择 log、Min-Max 或 Z-score 标准化方式。

    参数：
        df (pd.DataFrame): 原始数据表

    返回：
        pd.DataFrame: 仅包含归一化后的字段
    """

    # 定义归一化函数
    def min_max_normalize(series):
        return (series - series.min()) / (series.max() - series.min()) if series.max() != series.min() else np.zeros_like(series)

    def z_score_normalize(series):
        return (series - series.mean()) / series.std() if series.std() != 0 else np.zeros_like(series)

    def log_normalize(series):
        return np.log1p(series)  # 避免 log(0)

    # 定义关键词到归一化方法的映射
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

    # 存储归一化后的列
    normalized_data = {}

    # 获取所有数值列

    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()

    for col in numeric_cols:
        col_lower = col.lower()
        for keyword, func in normalization_rules.items():
            if keyword in col_lower:
                try:
                    df[col] = func(df[col])  # 直接替换原始列
                    break
                except Exception as e:
                    print(f"归一化字段 {col} 时出错：{e}")
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
    # 清洗和标准化数据
    df_1 = clean_data(df)
    df_cleaned =  normalize_uniswap_df(df)
    # construct new file name
    csv_filename = 'normalized_' + file.replace('.xlsx', '.csv')
   # save as CSV ，remove index
    df_cleaned.to_csv(csv_filename, index=False)
    print(f"sava as {csv_filename}")

