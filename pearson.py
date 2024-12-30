import numpy as np
import database
from pandas import DataFrame


db = database.database(host='10.101.10.66', user='root', password='123456', database='stock')
basic_sql = f"select * from stock_data where ts_code ='600019.SH' order by trade_date desc limit 30"
basic_data = db.read_data(basic_sql)
basic_data = DataFrame(basic_data, columns=['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'pre_close', 'change', 'pct_chg', 'vol', 'amount'])
basic_open=basic_data['open']
basic_high = basic_data['high']
basic_low = basic_data['low']
basic_close = basic_data['close']
basic_amount = basic_data['amount']
basic_vol = basic_data['vol']
basci_len = len(basic_data)


stock_list_sql = "select * from stock_list"
stock_list = db.read_data(stock_list_sql)
data = DataFrame(stock_list, columns=['ticker', 'name', 'is_active', 'exchange_code', 'country_code', 'currency_code','create_time'])
# 在每一行中如果exchange_code = XSHG行，将ticker的值后面加上.SH，如果exchange_code = XSHE则在ticker的值后面加上.SZ
copy = data.copy()
copy.loc[copy['exchange_code'] == 'XSHG', 'ticker'] += '.SH'
copy.loc[copy['exchange_code'] == 'XSHE', 'ticker'] += '.SZ'

for code in range(len(copy)):
    compare_sql = f"select * from stock_data where ts_code = '{copy.iloc[code]['ticker']}' "
    compare_data=db.read_data(compare_sql)
    trade_days  = len(compare_data)
    for i in range(0,trade_days, basci_len):
        #截取判空，滑动数据不满足range后跳出
        if i+basci_len>=trade_days:
            break
        stock_offer = compare_data[i:i + basci_len]  # 截取后的数据
        stock_offer_data = DataFrame(stock_offer, columns=['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'pre_close', 'change', 'pct_chg', 'vol', 'amount'])
        open_o = stock_offer_data['open']
        close_o = stock_offer_data['close']
        high_o = stock_offer_data['high']
        low_o = stock_offer_data['low']
        amount_o = stock_offer_data['amount']
        vol_o = stock_offer_data['vol']
        # /**-------------计算相关系数-------------------*/
        open_k = np.corrcoef(basic_open, open_o)[0][1]
        close_k = np.corrcoef(basic_close, close_o)[0][1]
        high_k = np.corrcoef(basic_high, high_o)[0][1]
        low_k = np.corrcoef(basic_low, low_o)[0][1]
        amount_k = np.corrcoef(basic_amount, amount_o)[0][1]
        vol_k = np.corrcoef(basic_vol, vol_o)[0][1]
        ave_k = (open_k + close_k + high_k + low_k + amount_k ) / 5
        # 计算有效交易天数
        # 计算相关系数都要大于0.8才算有效，打印代码，日期
        if ave_k > 0.8:
            print(f"代码是:{copy.iloc[code]['ticker']},日期是：{stock_offer_data.iloc[0]['trade_date']}-{stock_offer_data.iloc[-1]['trade_date']},相关系数：open:{open_k},close:{close_k},high:{high_k},low:{low_k},ave:{ave_k},amount:{amount_k}, vol:{vol_k}")

