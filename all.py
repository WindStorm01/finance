import requests
import pandas as pd
import pymysql
import time
# 上交所：XSHG，深交所：XSHE
url = f"https://tsanghi.com/api/fin/stock/XSHE/list?token=8d7df9c4fcb544b08d41772550e50ce4"
data = requests.get(url).json().get("data")
df =pd.DataFrame(data)
#print(df)

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
    columns = ','.join(df.columns)
    placeholders = ', '.join(['%s'] * len(df.columns))
    insert_sql = f"INSERT INTO stock_list ({columns}) VALUES ({placeholders})"
    start_time = time.time()
    batch_size = 1000
    total_rows = len(df)
    #获取dataframe中某一行存在NAN的值，将其所在行剔除
    df.dropna(inplace=True)
    #print(df.isnull().any())

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