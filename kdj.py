import pandas as pd
import numpy as np
def calculate_kdj(df, n=9, m1=3, m2=3):
    """
    计算股票的KDJ指标

    参数:
    df: DataFrame, 包含股票的日高价(high)、日低价(low)和收盘价(close)和日期(trade_date)
    n: int, RSV计算周期, 默认为9
    m1: int, K值的计算周期, 默认为3
    m2: int, D值的计算周期, 默认为3

    返回:
    DataFrame包含K、D、J三个指标值
    """

    # 计算RSV (Raw Stochastic Value)
    low_list = df['low'].rolling(window=n).min()
    high_list = df['high'].rolling(window=n).max()
    rsv = (df['close'] - low_list) / (high_list - low_list) * 100

    # 计算K值
    k = pd.DataFrame(rsv).ewm(com=m1 - 1, adjust=False).mean()

    # 计算D值
    d = pd.DataFrame(k).ewm(com=m2 - 1, adjust=False).mean()

    # 计算J值
    j = 3 * k - 2 * d

    # 组合KDJ数据，包含日期
    kdj = pd.DataFrame({
        'date': df['trade_date'],
        'K': k.values.flatten(),
        'D': d.values.flatten(),
        'J': j.values.flatten()
    })

    return kdj