import time

import database
from pandas import DataFrame
import kdj

db = database.database(host='10.101.10.66', user='root', password='123456', database='stock')
stock_list_sql = "select * from stock_list where ticker in ('002408','002121','002163')"
stock_list = db.read_data(stock_list_sql)
data = DataFrame(stock_list, columns=['ticker', 'name', 'is_active', 'exchange_code', 'country_code', 'currency_code','create_time'])
# 在每一行中如果exchange_code = XSHG行，将ticker的值后面加上.SH，如果exchange_code = XSHE则在ticker的值后面加上.SZ
copy = data.copy()
copy.loc[copy['exchange_code'] == 'XSHG', 'ticker'] += '.SH'
copy.loc[copy['exchange_code'] == 'XSHE', 'ticker'] += '.SZ'

for code in range(len(copy)):
    basic_sql = f"select * from stock_data where ts_code = '{copy.iloc[code]['ticker']}' order by trade_date desc limit 30"
    basic_data=db.read_data(basic_sql)
    df = DataFrame(basic_data,
                             columns=['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'pre_close',
                                      'change', 'pct_chg', 'vol', 'amount'])
    real_data = df[[ 'trade_date','close', 'high', 'low']]
    # 对real_data数据进行排序，trade_date值小的排前面
    real_data = real_data.sort_values(by='trade_date')
    kdj_data = kdj.calculate_kdj(real_data, 9, 3, 3)

    print("\nKDJ交易信号:")
    signals = kdj_data.copy()

    signals = signals.dropna()

    # 超买信号 (K和D同时大于80)
    signals.loc[(signals['K'] > 80) & (signals['D'] > 80), 'Signal'] = 'Sell'

    # 超卖信号 (K和D同时小于20)
    signals.loc[(signals['K'] < 20) & (signals['D'] < 20), 'Signal'] = 'Buy'

    #计算kdj金叉，死叉信号
    signals['K_diff'] = signals['K'].diff()
    signals['D_diff'] = signals['D'].diff()
    signals.loc[(signals['K_diff'] > 0) & (signals['D_diff'] > 0) & (signals['K_diff'] > signals['D_diff']), 'Signal'] = 'Buy'
    signals.loc[(signals['K_diff'] < 0) & (signals['D_diff'] < 0) & (signals['K_diff'] < signals['D_diff']), 'Signal'] = 'Sell'


    # 打印带有交易信号的结果
    print('代码:',{copy.iloc[code]['ticker']},'信号:',signals[signals['Signal'] != 'Hold'])
    time.sleep(0.1)