import datetime
# from dateutil.rrule import rrule, DAILY, MONTHLY
import shutil
import os
import glob
import zipfile
import sys
import pandas as pd
from tqdm import trange

"""





"""



class StandardizeData:
    def __init__(self):
        self.DATEs = ['sale_date','transaction_time']
    
    def Read_Data_csv(self):
        DATEs = self.DATEs
        
        RawData = os.path.join('RawData')
        if not os.path.isdir(RawData):
            os.mkdir(RawData)
    
        AllFile = [_ for _ in os.listdir(os.path.join("DataSource")) if _ != '.DS_Store']
    
        KeyNames = list(dict.fromkeys([_.split('_')[0] for _ in AllFile]))
        
        for KeyName in KeyNames:
            
            zip_names = [os.path.join("DataSource",_) for _ in AllFile if _.split('_')[0] == KeyName]
            #列出壓縮檔內檔名
            csv_names = [_.replace('.zip','.csv').split('/')[1] for _ in zip_names ]
        
            #完整檔名
            final_Name = os.path.join(RawData,f'{KeyName}_final.csv')
        
            #刪除舊檔
            if os.path.isfile(final_Name):
                os.remove(final_Name)
            
            
            #因為第一次需要表頭，第二次就不用，讓header ＝false
            frequency = 0
            for zip_name,csv_name in zip(zip_names,csv_names):  
                
                ExportHeader = True if frequency == 0 else False
                
                StandardizeData().MergeData(zip_name,csv_name,KeyName,final_Name,ExportHeader)
                frequency += 1
                # print(ExportHeader)
                
    
    def MergeData(self,zip_name,csv_name,KeyName,final_Name,ExportHeader=True):
        DATEs = self.DATEs
    
        with zipfile.ZipFile(zip_name,'r') as z:
            with z.open(csv_name) as f:
                print(f'正在合併 {csv_name}')
                
                df = StandardizeData().ReadFile(f,KeyName
                               ,chunksize = 10 ** 6
                               # ,DATEs=DATEs
                              )
    
                    
                for chunk in df:
                    if KeyName !='Items':
                        chunk['PrimaryKey']=chunk['store'] + chunk['TillID'] + chunk['TransactionId'] + chunk['OperatorID']  + chunk['transaction_time'].astype(str) 
                        chunk['PrimaryKey'] =chunk['PrimaryKey'].str.replace(' ','' )
                        chunk['PrimaryKey'] = chunk['PrimaryKey'].str.replace(' ','')
                        
                    chunk = StandardizeData().ETL(chunk,KEY=KeyName)
                    
                    chunk.to_csv(final_Name
                                 ,mode = 'a'
                                 ,encoding = "utf-8-sig"
                                  ,header = ExportHeader
                                 ,index = False)
    
    def ReadFile(self,FileName,KeyName,chunksize = None,):
        DATEs = self.DATEs
        
        if KeyName == 'Items':
            df = pd.read_csv(FileName
                              ,chunksize= chunksize
                             ,dtype = object
                             ) 
        else:
            df = pd.read_csv(FileName
                              ,chunksize= chunksize
                              ,parse_dates=DATEs
                             ,dtype = object
                             ) 
            
        return df
    
    def ETL(self,chunk,KEY):
        DATEs = self.DATEs
    
        
        Dtype = DtypeDict()
        
        for key,values in Dtype[KEY].items():
            for DATE in DATEs:
                if key != DATE:
        
                    if values == 'float64':
                        chunk[key].fillna(value=0, inplace=True)
                        chunk[key] = chunk[key].astype('float64')
                        
                    elif values == 'int64':
                        chunk[key].fillna(value=0, inplace=True)
                        chunk[key] = chunk[key].astype('int64')
                        
                    elif values == object :
                        chunk[key] = chunk[key].astype(str)
                        chunk[key] = chunk[key].str.replace(' ','')
                        # print(key)
                    else:
                        # print(key)
                        pass
        if KEY !='Items':
            chunk = chunk[chunk['store'] !='store']
        return chunk
                    

def MergeStoreNumber(left,right):
    
    right['代號'] = right['代號'].astype(str).str.replace('S','')
    right['代號'] = right['代號'].astype(int)
    left['store']=left['store'].astype(int)
    left = pd.merge(left=left
             ,right=right
             ,left_on='store'
             ,right_on='代號'
             )
    del left['代號']
    return left


def WriteSQL(SQL,Description):
    path = os.path.join('SQL')
    if not os.path.isdir(path):
        os.mkdir(path)
        
    
    path = open(os.path.join(path,f'{Description}.sql'),'w')
    path.write(SQL)
    path.close()

    
def DtypeDict():
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
            'own_label'                : object,
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
    return DtypeDict



# 用於提取時間區間
# def date_range(start_date, end_date):
#     return [dt.date() for dt in rrule(DAILY, dtstart=start_date, until=end_date)]


# def ETLforString(OldSing,NewSing,String):
#     for old,new in zip(OldSing,NewSing):
#         String = String.replace(old,new)
#     return String

# def Read_Data_csv_ForItems():
#     Files = [_ for _ in os.listdir(os.path.join('DataSource')) if _.split('_')[0] == 'Items']
#     # Files = [os.path.join('DataSource',_) for _ in Files]
#     Files.sort(reverse = True)
#     return Files[0]
