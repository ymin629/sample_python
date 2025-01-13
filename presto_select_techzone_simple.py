import prestodb
import pandas as pd

conn=prestodb.dbapi.connect(
    host='ibm-lh-lakehouse-prestissimo226-presto-svc-cpd-operands.apps.675fce85cede2185c41a617f.ocp.techzone.ibm.com',  ## https는 제외한다
    port=443,
    user='cpadmin',
    catalog='iceberg',
    schema='kym',
    http_scheme='https',
    auth=prestodb.auth.BasicAuthentication("cpadmin", "RkTyzx2Dgg6lw0HPfdwBjeiofq1sai8w")  ## TechZone에서 받은 계정과 비번
)
cur = conn.cursor()
cur.execute('SELECT * FROM iceberg.kym.test')
rows = cur.fetchall()

##print(rows)

rows = pd.DataFrame(rows)
print(rows)
