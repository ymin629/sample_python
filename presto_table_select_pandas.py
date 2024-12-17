import prestodb         ## presto library 불러오기 pip3 install로 미리 설치 필요
import pandas as pd     ## pandas library 불러오기 pip3 install로 미리 설치 필요

conn=prestodb.dbapi.connect(
    host='localhost',
    port=8443,
    user='ibmlhadmin',
    catalog='test_data',
    schema='ym',
    http_scheme='https',
    auth=prestodb.auth.BasicAuthentication("ibmlhadmin", "password")
)
conn._http_session.verify = '/Users/kym/Downloads/cert.crt'

cur = conn.cursor()
cur.execute("SELECT * FROM iceberg_data.kym.sample")
rows = cur.fetchall()
##rows = cur.fetchmany(5)

rows = pd.DataFrame(rows)
print(rows)
##print (rows[0])
##print (rows[1])