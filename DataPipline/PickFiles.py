import datetime
# from dateutil.rrule import rrule, DAILY, MONTHLY
import shutil
import os
import glob
import zipfile
import sys
import pandas as pd
from tqdm import trange






# 用於提取時間區間
# def date_range(start_date, end_date):
#     return [dt.date() for dt in rrule(DAILY, dtstart=start_date, until=end_date)]
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

def ETLforString(OldSing,NewSing,String):
    for old,new in zip(OldSing,NewSing):
        String = String.replace(old,new)
    return String

def Read_Data_csv_ForItems():
    Files = [_ for _ in os.listdir(os.path.join('DataSource')) if _.split('_')[0] == 'Items']
    # Files = [os.path.join('DataSource',_) for _ in Files]
    Files.sort(reverse = True)
    return Files[0]


def Read_Data_csv():


    RawData = os.path.join('RawData')
    if not os.path.isdir(RawData):
        os.mkdir(RawData)


    final_Name = "_.csv"
    Raw_data_files = ["Txn",'Inv','Refund','Void','Point']
    
    
    
    if os.path.isfile(os.path.join(RawData,f'Items_test.csv')):
            os.remove(os.path.join(RawData,f'Items_test.csv'))
    MergeData(
                zip_name = os.path.join("DataSource",Read_Data_csv_ForItems())
                ,csv_name = Read_Data_csv_ForItems().replace('zip','csv')
                ,Raw_data_file = 'Items'
                ,final_Name = final_Name
                ,RawData = RawData
              )
    
    for Raw_data_file in Raw_data_files:

        # 篩選檔案
        zip_names = [_ for _ in os.listdir(os.path.join("DataSource")) if _.split("_")[0] == Raw_data_file]

        #列出壓縮檔內檔名
        csv_names = [i.replace('.zip','.csv') for i in zip_names]

        #完整檔名
        zip_names = [os.path.join("DataSource",_) for _ in zip_names ]


        if os.path.isfile(os.path.join(RawData,f'{Raw_data_file}{final_Name}')):
            os.remove(os.path.join(RawData,f'{Raw_data_file}{final_Name}'))

        for zip_name,csv_name in zip(zip_names,csv_names):
            MergeData(zip_name,csv_name,Raw_data_file,final_Name,RawData)
            
            
            



def MergeData(zip_name,csv_name,Raw_data_file,final_Name,RawData):
    chunksize = 10 ** 6
    # if Raw_data_file != 'Items':
    #     _index = ['store', 'transaction_time']
    with zipfile.ZipFile(zip_name,'r') as z:
        with z.open(csv_name) as f:
            print(f'正在合併 {csv_name}')
            
            df = pd.read_csv(f 
                             # ,index_col=_index 
                              ,chunksize = chunksize
                             ,dtype = object) 
            
            
            for chunk in df:
                chunk = chunk.apply(lambda x:x.str.replace(' ',''))
                if Raw_data_file !='Items':
                    chunk['PrimaryKey']=chunk['store'] + chunk['TillID'] + chunk['TransactionId'] + chunk['OperatorID']  + chunk['transaction_time'] + chunk['transaction_time']
                    chunk['PrimaryKey'] =chunk['PrimaryKey'].str.replace(' ','' )
                
                chunk.to_csv(os.path.join(RawData,f'{Raw_data_file}{final_Name}') 
                             ,mode = 'a+'
                             ,encoding = "utf-8-sig"
                             ,index = False)



