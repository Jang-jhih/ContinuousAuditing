from DataPipline.PickFiles import *
import time
import datetime
import sqlite3
import os
import pandas as pd
# from tqdm import tqdm
import mysql.connector
from mysql.connector import Error
# import pandas as pd
import sqlalchemy


class SQL:
    def __init__(self,file,filepath,ConcatTable,TEST=False):
        self.filepath = filepath
        self.file = file
        self.TEST = TEST
        
   
        self.ChoseKey = self.file.split('_')[0]
        self.ConcatTable = ConcatTable
        # self.ConcatTable = pd.read_excel(os.path.join('RawData','店總表.xlsx')
                       # ,usecols = ['代號', '門市']) 
               
        self.pattern = pattern
        

        self.con = sqlite3.connect('DW.db')
        self.cursor = self.con.cursor()
            

    
    
    
    
    def AutoCreatTable(self,PrimaryKey=None):
        file = self.file
        filepath = self.filepath
        ChoseKey = self.ChoseKey
        
        # for file,filepath,ChoseKey in zip(Files,FilesPath,ChoseKeys):
        if self.TEST == True:
            print(f'{file}')
        
        DataName = []
        Columns = []
        dtype = []
        
        TMPFilePath = pd.read_csv(filepath,nrows = 1 ,dtype=SelectDtype(ChoseKey) )
        
        def CheckStoreColumns(Columns,store = 'store'):
            for Column in Columns:
                if Column == store:
                    return True
            return False
        
        if CheckStoreColumns(TMPFilePath.columns) == True:
            TMPFilePath = MergeStoreNumber(left=TMPFilePath,right=self.ConcatTable)
        
   
        
        for columns,Dtype in zip(TMPFilePath.columns,TMPFilePath.dtypes):
            DataName.append(file)
            Columns.append(columns)
            dtype.append(Dtype)
    
        df = pd.DataFrame({"DataName":DataName,"Columns":Columns,"dtype":dtype})
        df.set_index("DataName", inplace = True)
        df = df.astype(str)
    
    

        Columns = []
        for key,values in df.loc[file]['Columns'].items():
            Columns.append(values)

        dtype = []
        for key,values in df.loc[file]['dtype'].items():
            dtype.append(values)

        SqlSchema = []
        for a,b in zip(Columns,dtype):
            SqlSchema.append(f'`{a}` {b}')
        
        SqlSchema = str(tuple(SqlSchema)).replace("'",'')
        
        # TableName = file.split('_')[0]
        SQL = f'CREATE TABLE IF NOT EXISTS `{ChoseKey}` {SqlSchema}'
        
        if PrimaryKey != None:
            SQL=SQL.replace('`PrimaryKey` object','`PrimaryKey` object PRIMARY KEY ASC')
        
        SQL=SQL.replace(',',',\n')
        
        ChangeType_Old = ['float64','int64','object']
        ChangeType_New = ['float','int','varchar(255)']
        
        for old,new in zip(ChangeType_Old,ChangeType_New):
            SQL = SQL.replace(old,new)
            
        print(SQL)
        WriteSQL(SQL,'Schema')
        # con = sqlite3.connect('DW.db')
        # cursor = con.cursor()
        self.cursor.execute(SQL)
        
        # if PrimaryKey != None:
        #     cursor.execute(f"ALTER TABLE {TableName} ADD PRIMARY KEY ({PrimaryKey});")

                
                
    def InsertData(self):
        file = self.file
        filepath = self.filepath
        ChoseKey = self.ChoseKey

    
        chunksize = 10 ** 6
        df = pd.read_csv(filepath
                            , chunksize=chunksize
                            ,low_memory=False
                          )

        for chunk in df:
            chunk.fillna('', inplace=True)
            
            VALUES_ = []
            for _ in range(0,len(chunk.columns)):
                VALUES_.append('?')
            
            VALUES_ = str(tuple(VALUES_))
            VALUES_ = ETLforString(["'"],[""],VALUES_)
            Columns = ETLforString(["'"],["`"],str(tuple(chunk.columns)))
        
            
            chunk.to_sql(ChoseKey,
                          con=self.con, 
                          index=False, 
                          if_exists='append')

def LoginMysql(localhost='localhost',
               database='DataWarehouse',
               user='root',
               password='Aaa710258' ):
    try:
        # 連接 MySQL/MariaDB 資料庫
        connection = mysql.connector.connect(
            host=localhost,          # 主機名稱
            database=database, # 資料庫名稱
            user=user,        # 帳號
            password=password)  # 密碼
    
        if connection.is_connected():
    
            # 顯示資料庫版本
            db_Info = connection.get_server_info()
            print("資料庫版本：", db_Info)
    
            # 顯示目前使用的資料庫
            cursor = connection.cursor()
            cursor.execute("SELECT DATABASE();")
            record = cursor.fetchone()
            print("目前使用的資料庫：", record)
    
    except Error as e:
        print("資料庫連接失敗：", e)
    
    return connection
    
    # finally:
    #     if (connection.is_connected()):
    #         cursor.close()
    #         connection.close()
    #         print("資料庫連線已關閉")

