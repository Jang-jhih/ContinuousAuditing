from DataPipline.PickFiles import *from ControlSQL.CreatTable import *Read_Data_csv()AutoCreatTable()con = sqlite3.connect('DW.db')cursor = con.cursor()Files = os.listdir(os.path.join('RawData'))dfs = [os.path.join('RawData',_) for _ in Files if _.split('.')[1] == 'csv']chunksize = 10 ** 6for File in Files:    ChoseKey = File.split('_')[0]        df = pd.read_csv(dfs[0]                      # ,nrows = 100                      ,dtype = SelectDtype(ChoseKey)                       , chunksize=chunksize                     )        for chunk in df:        chunk.fillna('', inplace=True)                # chunk = chunk.apply(lambda x:[_.replace(',','') for _ in x])                        VALUES_ = []        for _ in range(0,len(chunk.columns)):            VALUES_.append('?')                VALUES_ = str(tuple(VALUES_))                VALUES_ = ETLforString(["'"],[""],VALUES_)        Columns = ETLforString(["'"],["`"],str(tuple(chunk.columns)))            chunk.to_sql(ChoseKey,                      con=con,                       index=False,                       if_exists='append')        chunk['tran_tendered']