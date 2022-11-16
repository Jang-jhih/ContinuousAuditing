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

# def ETL(df):

import pandas  as pd
import os



# KEY = 'Inv'


def ReadFile(FileName,KEY,chunksize=None):
    
    DATEs = ['sale_date','transaction_time']
    df = pd.read_csv(FileName,
                     chunksize=None,
                     dtype = object,
                     parse_dates= DATE
                     )

    for key,values in DtypeDict[KEY].items():
        for DATE in DATEs:
            if key != DATE:
    
                if values == 'float64':
                    df[key].fillna(value=0, inplace=True)
                    df[key] = df[key].astype('float64')
                    
                elif values == 'int64':
                    df[key].fillna(value=0, inplace=True)
                    df[key] = df[key].astype('int64')
                    
                elif values == object :
                    df[key] = df[key].astype(str)
                    df[key] = df[key].str.replace(' ','')
                    # print(key)
                else:
                    print(key)
    
    return df
    
    DtypeDict = {
        'Inv' : {
            'store'               : object,  
            'sale_date'           : 'datetime64[ns]',   
            'TillID'              : object,  
            'transaction_time'    : 'datetime64[ns]',   
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
            'sale_date'                 : 'datetime64[ns]',
            'TillID'                    : object,
            'transaction_time'          : 'datetime64[ns]',
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
            'sale_date'                 :'datetime64[ns]',
            'TillID'                    :object,
            'transaction_time'          :'datetime64[ns]',
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
            'sale_date'                 : 'datetime64[ns]',
            'TillID'                    : object,
            'transaction_time'          : 'datetime64[ns]',
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
            'sale_date'                 : 'datetime64[ns]',
            'TillID'                    : object,
            'transaction_time'          : 'datetime64[ns]',
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












#%%

for _ in DATE:    
    df[_]=pd.to_datetime(df[_])

STR = ['prom_desc','credit_carditem_code','void_type']
for _ in STR:
    try:
        df[_]=df[_].astype(str)
    except:
        pass


INT = ['store','TillID','TransactionId','GlobalTxnID','OperatorID','MediaType'
       ,'CardNo','voucher_used','item_cdesc','own_label','vendor_code','sup_code'
       ,'sup_cname','dept_code','class_code','promotion_id','subclass_codestore'
       ,'points_earnedstore', 'RsGlobalTxnID','item_code','voucher_usedstore']

for _ in INT:
    try:
        df[_]=df[_].astype(int)
    except:
        pass

FLOAT = ['tran_tendered','Tendered','cost','stock_cost','soh_qty',
'Quantity','price','discounted_price','discount']

for _ in FLOAT:
    try:
        df[_]=df[_].astype(int)
    except:
        pass



for key,values in dict(df.dtypes).items():
    if values == 'object':
        print(key,values)
        df[key] = df[key].str.replace(' ','')







