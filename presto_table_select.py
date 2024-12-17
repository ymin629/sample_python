import prestodb

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

##print(rows)
##print (rows[0])
##print (rows[1])

header = ""
for column in cur.description:
    if (header == ""):
        header = '{!s:<15s}'.format(column[0])
    else:
        header = '{0} {1!s:<15s}'.format(header,column[0])
print(header)

count = 0
for row in rows:
    count += 1
    line = ""
    for column in row:
        if (line == ""):
            line = '{!s:<15s}'.format(column)
        else:
            line = '{0} {1!s:<15.15s}'.format(line,column)
    if (count == 10):
        break
    print(line,end="\n")