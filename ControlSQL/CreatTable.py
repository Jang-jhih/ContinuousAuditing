import time
import datetime
import sqlite3
import os
import pandas as pd

def AutoCreatTable():
    Files = [_ for _ in os.listdir(os.path.join('RawData')) if _.split('.')[1]=='csv']
    FilesPath = [os.path.join('RawData',_) for _ in Files if _.split('.')[1]=='csv']
    
    
    
    DataName = []
    Columns = []
    dtype = []
    for File,FilePath in zip(Files,FilesPath):
        ChoseKey = File.split('_')[0]
        
        TMPFilePath = pd.read_csv(FilePath,nrows = 1 ,dtype=SelectDtype(ChoseKey))
        for columns,Dtype in zip(TMPFilePath.columns,TMPFilePath.dtypes):
            DataName.append(File)
            Columns.append(columns)
            dtype.append(Dtype)

    df = pd.DataFrame({"DataName":DataName,"Columns":Columns,"dtype":dtype})
    df.set_index("DataName", inplace = True)
    df = df.astype(str)

    for filename in Files:
        print(filename)
        # ChoseKey = filename.split('_')[0]1
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
        
        TableName = filename.split('_')[0]
        SQL = f'CREATE TABLE IF NOT EXISTS `{TableName}` {SqlSchema}'
        
        
        
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
        'TransactionId'       : object    ,  
        'GlobalTxnID'         : object    ,  
        'OperatorID'          : object    ,  
        'tran_tendered'       : 'float64'  ,    
        'MediaType'           : object    ,  
        'Tendered'            : 'float64'  ,    
        'CardNo'              : object    ,  
        'voucher_used'        : 'float64'  ,    
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

