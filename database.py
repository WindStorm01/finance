import pymysql
from pandas import DataFrame
import time
import tushare as ts


class database:

    def __init__(self, host: str, user: str, password: str, database: str, port: int = 3306):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.connect()  # 初始化连接

    def connect(self):
        """建立数据库连接"""
        self.conn = pymysql.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database,
            port=self.port
        )
        self.cursor = self.conn.cursor()

    def ensure_connection(self):
        """确保数据库连接可用"""
        try:
            self.conn.ping(reconnect=True)
        except:
            self.connect()
    def batch_insert(self, sql: str, data: DataFrame, batch_size: int = 3000):
        self.ensure_connection()  # 确保连接可用

        try:
            start_time = time.time()
            batch_size = 1000
            total_rows = len(data)
            for i in range(0, total_rows, batch_size):
                batch_data = data.iloc[i:i + batch_size].values.tolist()
                self.cursor.executemany(insert_sql, batch_data)
                self.conn.commit()
                #print(f"已处理 {min(i + batch_size, total_rows)}/{total_rows} 行")
            end_time = time.time()
            #print(f"Method 2 完成写入 {total_rows} 行数据，用时：{end_time - start_time:.2f} 秒")
        except Exception as e:
            print(f"写入失败：{str(e)}")
            self.conn.rollback()

    def read_data(self, sql: str) -> DataFrame:
        self.ensure_connection()  # 确保连接可用
        try:
            start_time = time.time()
            self.cursor.execute(sql)
            data = self.cursor.fetchall()
            end_time = time.time()
            #print(f"Method 1 读取 {len(data)} 行数据，用时：{end_time - start_time:.2f} 秒")
            return data
        except Exception as e:
            print(f"读取失败：{str(e)}")

    def close(self):
        """关闭数据库连接"""
        if hasattr(self, 'cursor') and self.cursor:
            self.cursor.close()
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()



if __name__ == '__main__':
    db = database(host='10.101.10.66', user='root', password='123456', database='stock')
    sql = f"SELECT * FROM stock_list"
    data = db.read_data(sql)
    data = DataFrame(data, columns=['ticker', 'name', 'is_active', 'exchange_code', 'country_code', 'currency_code',
                                    'create_time'])
    # 在每一行中如果exchange_code = XSHG行，将ticker的值后面加上.SH，如果exchange_code = XSHE则在ticker的值后面加上.SZ
    data.drop(columns=['create_time'], inplace=True)
    copy = data.copy()
    copy.loc[copy['exchange_code'] == 'XSHG', 'ticker'] += '.SH'
    copy.loc[copy['exchange_code'] == 'XSHE', 'ticker'] += '.SZ'

    # 查询tushare数据
    ts.set_token('683ab341260fb4212d86070053b29ceda3b9c8bc49d8b8713fa09e1d')
    pro = ts.pro_api()

    copy.dropna(inplace=True)
    columns = 'ts_code,trade_date,open,high,low,close,pre_close,`change`,pct_chg,vol,amount'
    #columns = ','.join(
    #    f'`{col}`' if col.lower() in ['change', 'default', 'status', 'group', 'order', 'select', 'where'] else col
    #    for col in copy.columns)
    #placeholders = ', '.join(['%s'] * len(copy.columns))
    placeholders = ','.join(['%s'] * len(columns.split(',')))
    insert_sql = f"INSERT INTO stock_data({columns}) VALUES ({placeholders})"

    # 获取002121.SZ的日线数据
    # df = pro.daily(ts_code='002121.SZ', start_date='19700101', end_date='20241226')
    # 通过copy变量中的ticker列的值，传递给pro.daily函数，获取日线数据
    for i in range(len(copy)):
        print(copy.iloc[i]['name'])
        df = pro.daily(ts_code=copy.iloc[i]['ticker'],start_date='20241227')
        # 写入数据库 stock_data 表
        db.batch_insert(insert_sql, df)
        # sleep100毫秒，防止请求过快
        time.sleep(0.1)
    print("写入完成")


