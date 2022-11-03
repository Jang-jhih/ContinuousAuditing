import pandas as pd
import os
import sqlite3


def ETLforString(OldSing,NewSing,String):
    for old,new in zip(OldSing,NewSing):
        String = String.replace(old,new)
    return String

con = sqlite3.connect('DW.db')
cursor = con.cursor()

dfs = [os.path.join('DataSource',_) for _ in os.listdir(os.path.join('DataSource')) if _.split('.')[1] == 'csv']



sql = '''CREATE TABLE IF NOT EXISTS `transaction`( 
`交易時間` date,
`訂單編號` TEXT,
`商戶交易編號` TEXT,
`銀行交易序號` TEXT,
`商戶名稱` TEXT,
`品牌名稱` TEXT,
`商店名稱` TEXT,
`交易類型` TEXT,
`交易狀態` TEXT,
`交易幣別` TEXT,
`訂單金額` INTEGER,
`全點折抵` INTEGER,
`支付金額` INTEGER,
`點數不足扣款金額` INTEGER,
`回饋全點` INTEGER,
`手機號碼` TEXT,
`付款方式` TEXT,
`銀行` TEXT
)'''
cursor.execute(sql)





chunksize = 10 ** 6



df = pd.read_csv(dfs[0],dtype=object , chunksize=chunksize)

for chunk in df:
    chunk.fillna('', inplace=True)
    
    chunk = chunk.apply(lambda x:[_.replace(',','') for _ in x])
    
    
    chunk['交易時間'] = pd.to_datetime(chunk['交易時間'])
    chunk['訂單金額'] = chunk['訂單金額'].astype(int)
    chunk['全點折抵'] = chunk['全點折抵'].astype(int)
    chunk['支付金額'] = chunk['支付金額'].astype(int)
    chunk['點數不足扣款金額'] = chunk['點數不足扣款金額'].astype(int)
    chunk['回饋全點'] = chunk['回饋全點'].astype(int)
    
    
    
    VALUES_ = []
    for _ in range(0,len(chunk.columns)):
        VALUES_.append('?')
    
    VALUES_ = str(tuple(VALUES_))
    
    VALUES_ = ETLforString(["'"],[""],VALUES_)
    Columns = ETLforString(["'"],["`"],str(tuple(chunk.columns)))

    chunk.to_sql
    chunk.to_sql('transaction',
                  con=con, 
                  index=False, 
                  if_exists='append')




sql_coulmns = f"SELECT * "
sql_table = f"FROM `transaction`"
sql_where = f"WHERE `交易時間` between '2022-10-10' and '2022-10-11'"
sql = f"{sql_coulmns} {sql_table} {sql_where}"

df2 = pd.read_sql(sql,con)
