import datetime
from dateutil.rrule import rrule, DAILY, MONTHLY
import shutil
import os
import glob
import zipfile
import sys
import pandas as pd


# 用於提取時間區間
def date_range(start_date, end_date):
    return [dt.date() for dt in rrule(DAILY, dtstart=start_date, until=end_date)]


# 用於複製資料
def Copy_File_list(days):
#Rawdata路徑
    old_path = r"\\wtctw-qnap01\wtctwnasqnap\IA\ACL\Void&Refund\Rawdata"                                
    #目的路徑
    new_path = "C:/ACL/_DataWarehouse_2022/DataSource"                                                  
    #列出資料夾清單
    dir_list = os.listdir(old_path)                                                                     
    #排序資料夾清單，並選取最後一個，選擇最新的資料
    dir_list = sorted([i for i in dir_list if i.split('_')[0] == "Items"])[-1]                          
    #顯示檔案名稱，讓使用者知道抓了什麼資料
    print(dir_list)                                                                                     
    #複製檔案
    shutil.copyfile(old_path+'\\'+dir_list , new_path +'/'+dir_list)                                    
#檔案名稱`,存放檔案名稱
    file_name = ["Txn",'Inv','Refund','Void','Point']                                                   
    #For loop 用於逐一處理List
    for file in file_name:                                                                              
        #取得日期區間
        _data_range_limit = datetime.datetime.today()-datetime.timedelta(days = days)                   
        _date_range = date_range(_data_range_limit,datetime.date.today()+datetime.timedelta(days=  1))  
        #列出檔名，這些檔名為list
        _file_name = []                                                                                 
        for data in _date_range:
            _file_name.append(file + "_" + data.strftime('%Y%m%d') +".zip")
#把檔名放入for loop 處理
        for _file in _file_name:                                                                        
            try:                                                                                        #因為檔名是連續的，但Rawdata是一周一周的
                shutil.copyfile(old_path+'\\'+_file , new_path +'\\'+_file)                             #會有複製不到檔案的狀況，所以用例外處理，
                print(_file)                                                                            #假如沒有看到對應的檔案就會跳過。
            except Exception:
                pass

def Read_Data_csv_ForItems():
    Files = [_ for _ in os.listdir(os.path.join('DataSource')) if _.split('_')[0] == 'Items']
    Files = [os.path.join('DataSource',_) for _ in Files]
    Files.sort(reverse = True)
    return Files[0]


def Read_Data_csv():
    chunksize = 10 ** 6
    
    RawData = os.path.join('RawData')
    if not os.path.isdir(RawData):
        os.mkdir(RawData)
    
    # ,'Items'
    final_Name = "_test.csv"
    Raw_data_files = ["Txn",'Inv','Refund','Void','Point']
    for Raw_data_file in Raw_data_files:
        
        # 篩選檔案
        zip_names = [_ for _ in os.listdir(os.path.join("DataSource")) if _.split("_")[0] == Raw_data_file]
        
        #列出壓縮檔內檔名
        csv_names = [i.replace('.zip','.csv') for i in zip_names]
        
        #完整檔名
        zip_names = [os.path.join("DataSource",_) for _ in zip_names ]

        
        if os.path.isfile(Raw_data_file+final_Name):
            os.remove(Raw_data_file+final_Name)

        _index = ['store', 'transaction_time']

        for zip_name,csv_name in zip(zip_names,csv_names):
            with zipfile.ZipFile(zip_name,'r') as z:
                with z.open(csv_name) as f:
                    print(f'正在合併 {csv_name}')
                    df = pd.read_csv(f ,index_col=_index ,chunksize = chunksize) 
                    for chunk in df:
                        chunk.to_csv(os.path.join(RawData,f'{Raw_data_file}{final_Name}') ,mode = 'a+')



# days = input('以今日往前計算，請輸入資料[天數] ：')                                                 #輸入資料天數
# days = int(days)                                                                                    #輸入的內容都會是文字，所以轉成數值以便計算
# days = 90
# print("開始下載資料")
# argv = int(sys.argv[1])*7
# days = argv



# Copy_File_list(days)
# Copy_File_list :套件名稱
# days           :傳入變數