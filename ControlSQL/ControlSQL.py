from DataPipline.PickFiles import *
import time
import datetime
import sqlite3
import os
import pandas as pd
from tqdm import tqdm




class SQL:
    def __init__(self,file,filepath,ConcatTable,TEST=False):
        self.filepath = filepath
        self.file = file
        self.TEST = TEST
        self.con = sqlite3.connect('DW.db')
        self.cursor = self.con.cursor()
        self.ChoseKey = self.file.split('_')[0]
        self.ConcatTable = ConcatTable
        # self.ConcatTable = pd.read_excel(os.path.join('RawData','店總表.xlsx')
                       # ,usecols = ['代號', '門市']) 
    
    
    
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
        WriteSQL(SQL,Schema)
        # con = sqlite3.connect('DW.db')
        # cursor = con.cursor()
        self.cursor.execute(SQL)
        
        # if PrimaryKey != None:
        #     cursor.execute(f"ALTER TABLE {TableName} ADD PRIMARY KEY ({PrimaryKey});")

                
                
    def InsertData(self):
        file = self.file
        filepath = self.filepath
        ChoseKey = self.ChoseKey
        
        # for file,filepath,ChoseKey in zip(Files,FilesPath,ChoseKeys):
        
    
        chunksize = 10 ** 6
        # ChoseKey = file.split('_')[0]
        # print(f'塞入{ChoseKey}')
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



def SelectDtype(FileName):
    DtypeDict = {
    'Inv' : {
        'store'               : object,  
        'sale_date'           : object,   
        'TillID'              : object,  
        'transaction_time'    : object,   
        'TransactionId'       : object,  
        'GlobalTxnID'         : object,  
        'OperatorID'          : object,  
        'tran_tendered'       : 'float64',
        'MediaType'           : object,  
        'Tendered'            : 'float64',    
        'CardNo'              : object,  
        'voucher_used'        : 'float64',    
        'credit_card'         : object
         },
    
    'Items' : {
        'item_code'                : object,
        'item_cdesc'               : object,
        'cost'                     : 'float64',
        'own_label'                : 'float64',
        'vendor_code'              : object,
        'sup_code'                 : object,
        'sup_cname'                : object,
        'dept_code'                : object,
        'class_code'               : object,
        'subclass_code'            : object
    },
    
    'Point' :{
        'store'                     : object,
        'sale_date'                 : object,
        'TillID'                    : object,
        'transaction_time'          : object,
        'TransactionId'             : object,
        'GlobalTxnID'               : object,
        'OperatorID'                : object,
        'tran_tendered'             : 'float64',
        'CardNo'                    : object,
        'promotion_id'              : object,
        'prom_desc'                 : object,
        'points_earned'             : 'float64'
    },
    
    
    'Refund' : {
        'store'                     :object,
        'sale_date'                 :object,
        'TillID'                    :object,
        'transaction_time'          :object,
        'TransactionId'             :object,
        'RsGlobalTxnID'             :object,
        'tran_tendered'             :'float64',
        'OperatorID'                :object,
        'item_code'                 :object,
        'stock_cost'                :'float64',
        'soh_qty'                   :'int64',
        'Quantity'                  :'int64',
        'price'                     :'float64',
        'discounted_price'          :'float64',
        'CardNo'                    :object,
        'voucher_used'              :'float64'
    },
    
    'Txn' :{
        'store'                     : object,
        'sale_date'                 : object,
        'TillID'                    : object,
        'transaction_time'          : object,
        'TransactionId'             : object,
        'GlobalTxnID'               : object,
        'tran_tendered'             : 'float64',
        'OperatorID'                : object,
        'item_code'                 : object,
        'stock_cost'                : 'float64',
        'soh_qty'                   : 'int64',
        'Quantity'                  : 'int64',
        'price'                     : 'float64',
        'discounted_price'          : 'float64',
        'discount'                  : 'float64',
        'CardNo'                    : object,
        'voucher_used'              : 'float64'
    },
    
    'Void' : {
        'store'                     : object,
        'sale_date'                 : object,
        'TillID'                    : object,
        'transaction_time'          : object,
        'TransactionId'             : object,
        'tran_tendered'             : 'float64',
        'OperatorID'                : object,
        'item_code'                 : 'int64',
        'stock_cost'                : 'float64',
        'soh_qty'                   : 'int64',
        'Quantity'                  : 'int64',
        'price'                     : 'float64',
        'discounted_price'          : 'float64',
        'void_type'                 : object,
        'CardNo'                    : object
    }
    }
    return DtypeDict[FileName]

