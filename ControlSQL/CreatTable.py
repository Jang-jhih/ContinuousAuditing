import time
import datetime
import sqlite3
import os
import pandas as pd

def AutoCreatTable():
    Files = os.listdir(os.path.join('RawData'))
    FilesPath = [os.path.join('RawData',_) for _ in Files]
    
    
    
    DataName = []
    Columns = []
    dtype = []
    for File,FilePath in zip(Files,FilesPath):
        ChoseKey = File.split('_')[0]
        
        TMPFilePath = pd.read_csv(FilePath,nrows = 1,dtype=SelectDtype(ChoseKey))
        for columns,Dtype in zip(TMPFilePath.columns,TMPFilePath.dtypes):
            DataName.append(File)
            Columns.append(columns)
            dtype.append(Dtype)

    df = pd.DataFrame({"DataName":DataName,"Columns":Columns,"dtype":dtype})
    df.set_index("DataName", inplace = True)
    df = df.astype(str)

    for filename in Files:
    # key = 'Inv_test.csv'

        Columns = []
        for key,values in df.loc[filename]['Columns'].items():
            Columns.append(values)

        dtype = []
        for key,values in df.loc[filename]['dtype'].items():
            dtype.append(values)

        SqlSchema = []
        for a,b in zip(Columns,dtype):
            SqlSchema.append(f'`{a}` {b}')
        
        
        SqlSchema = str(tuple(SqlSchema)).replace("'",'')
        
        SQL = f'CREATE TABLE IF NOT EXISTS `{filename}` {SqlSchema}'
        
        
        
        # df.groupby("dtype").size().tolist()
        ChangeType_Old = ['float64','int64','object']
        ChangeType_New = ['float','int','varchar(255)']
        
        for old,new in zip(ChangeType_Old,ChangeType_New):
            SQL = SQL.replace(old,new)
            
            
        con = sqlite3.connect('DW.db')
        cursor = con.cursor()
        cursor.execute(SQL)




def SelectDtype(FileName):
    DtypeDict = {
    'Inv' : {
        'store'               : object    ,  
        'sale_date'           : object   ,   
        'TillID'              : object    ,  
        'transaction_time'    : object   ,   
        'TransactionId'       : 'int64'    ,  
        'GlobalTxnID'         : 'int64'    ,  
        'OperatorID'          : 'int64'    ,  
        'tran_tendered'       : 'float64'  ,    
        'MediaType'           : 'int64'    ,  
        'Tendered'            : 'float64'  ,    
        'CardNo'              : 'int64'    ,  
        'voucher_used'        : 'float64'  ,    
        'credit_card'         : 'float64'  
    },
    
    'Items' : {
        'item_code'                : 'int64',
        'item_cdesc'               : object,
        'cost'                     : 'float64',
        'own_label'                : 'float64',
        'vendor_code'              : 'int64',
        'sup_code'                 : object,
        'sup_cname'                : object,
        'dept_code'                : 'int64',
        'class_code'               : 'int64',
        'subclass_code'            : 'int64'
    },
    
    'Point' :{
        'store'                     : 'int64',
        'sale_date'                 : object,
        'TillID'                    : 'int64',
        'transaction_time'          : object,
        'TransactionId'             : 'int64',
        'GlobalTxnID'               : 'int64',
        'OperatorID'                : 'int64',
        'tran_tendered'             : 'float64',
        'CardNo'                    : 'int64',
        'promotion_id'              : 'int64',
        'prom_desc'                 : object,
        'points_earned'             : 'float64'
    },
    
    
    'Refund' : {
        'store'                     :'int64',
        'sale_date'                 :object,
        'TillID'                    :'int64',
        'transaction_time'          :object,
        'TransactionId'             :'int64',
        'RsGlobalTxnID'             :'int64',
        'tran_tendered'             :'float64',
        'OperatorID'                :'int64',
        'item_code'                 :'int64',
        'stock_cost'                :'float64',
        'soh_qty'                   :'int64',
        'Quantity'                  :'int64',
        'price'                     :'float64',
        'discounted_price'          :'float64',
        'CardNo'                    :'int64',
        'voucher_used'              :'float64'
    },
    
    'Txn' :{
        'store'                     : 'int64',
        'sale_date'                 : object,
        'TillID'                    : 'int64',
        'transaction_time'          : object,
        'TransactionId'             : 'int64',
        'GlobalTxnID'               : 'int64',
        'tran_tendered'             : 'float64',
        'OperatorID'                : 'int64',
        'item_code'                 : 'int64',
        'stock_cost'                : 'float64',
        'soh_qty'                   : 'int64',
        'Quantity'                  : 'int64',
        'price'                     : 'float64',
        'discounted_price'          : 'float64',
        'discount'                  : 'float64',
        'CardNo'                    : 'int64',
        'voucher_used'              : 'float64'
    },
    
    'Void' : {
        'store'                     : 'int64',
        'sale_date'                 : object,
        'TillID'                    : 'int64',
        'transaction_time'          : object,
        'TransactionId'             : 'int64',
        'tran_tendered'             : 'float64',
        'OperatorID'                : 'int64',
        'item_code'                 : 'int64',
        'stock_cost'                : 'float64',
        'soh_qty'                   : 'int64',
        'Quantity'                  : 'int64',
        'price'                     : 'float64',
        'discounted_price'          : 'float64',
        'void_type'                 : object,
        'CardNo'                    : 'int64'
    }
    }
    return DtypeDict[FileName]

