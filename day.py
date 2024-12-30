import tushare as ts
import pymysql
import time

ts.set_token('683ab341260fb4212d86070053b29ceda3b9c8bc49d8b8713fa09e1d')
pro = ts.pro_api()
#pro = ts.pro_api('683ab341260fb4212d86070053b29ceda3b9c8bc49d8b8713fa09e1d')
df = pro.daily(ts_code='002121.SZ')
print(df)

db_config = {
    'host': '10.101.10.66',
    'port': 3306,
    'user': 'root',
    'password': '123456',
    'database': 'stock',
    'charset': 'utf8'
}

try:
    # 打开数据库连接value)
    conn = pymysql.connect(**db_config)
    cursor = conn.cursor()
    columns = 'ts_code,trade_date,open,high,low,close,pre_close,`change`,pct_chg,vol,amount'
    placeholders = ', '.join(['%s'] * len(df.columns))
    insert_sql = f"INSERT INTO stock_data ({columns}) VALUES ({placeholders})"
    start_time = time.time()
    batch_size = 1000
    total_rows = len(df)
    for i in range(0, total_rows, batch_size):
        batch_data = df.iloc[i:i + batch_size].values.tolist()
        cursor.executemany(insert_sql, batch_data)
        conn.commit()
        print(f"已处理 {min(i + batch_size, total_rows)}/{total_rows} 行")

    end_time = time.time()
    print(f"Method 2 完成写入 {total_rows} 行数据，用时：{end_time - start_time:.2f} 秒")
except Exception as e:
    conn.rollback()
    print(f"写入失败：{str(e)}")
finally:
    cursor.close()
    conn.close()
