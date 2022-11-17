# from DataPipline.PickFiles import *
from ControlSQL.ControlSQL import *


#%%
import mysql.connector
from mysql.connector import Error
        # 主機名稱


 


connection = LoginMysql(localhost,database,user,password)
cursor = connection.cursor()
cursor.execute('slect * from inv;')


 
chunk.to_sql(ChoseKey,
              con=con, 
              index=False, 
              if_exists='append')
            
chunk.to_sql(ChoseKey, 
          con = con, 
          if_exists = 'append',
          index = False, chunksize = 1000)


#%%
import pandas as pd
import os
t = 'Txn'
final_Name =os.path.join('RawData',f'{t}_final.csv')

df = pd.read_csv(final_Name,usecols=['store'])
df['store'].astype(int)
df = df[df['store']!='store']
# df.shape
