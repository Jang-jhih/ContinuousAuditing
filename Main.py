from DataPipline.PickFiles import *from ControlSQL.ControlSQL import *from ORM.ORM import *StandardizeData().Read_Data_csv()Files = [_ for _ in os.listdir(os.path.join('RawData')) if _.split('.')[1]=='csv']FilesPath = [os.path.join('RawData',_) for _ in Files if _.split('.')[1]=='csv']pattern = 'mysql'for file,filepath in zip(Files,FilesPath):    # print(file,filepath)        if pattern == 'sqlite':        SQLobject = SQL(file,filepath,ConcatTable)        SQLobject.AutoCreatTable()        SQLobject.InsertData()    if pattern == 'mysql':        localhost='localhost'        database='DataWarehouse'        user='root'        password='Aaa710258'                         ChoseKey = file.split('_')[0]                # engine = LoginMysql()               engine=sqlalchemy.create_engine(f"mysql+pymysql://{user}:{password}@localhost:3306/{database}")        cnx = engine.connect()                                df = StandardizeData().ReadFile(FileName=filepath,KeyName=ChoseKey,chunksize = 10 ** 6)                print(f'正在塞入{filepath}')        for chunk in df:            chunk = StandardizeData().ETL(chunk,ChoseKey)                                    # chunk = sqlcol(chunk)                        chunk.to_sql(ChoseKey                           , cnx                           , if_exists='replace'                           , index=False)#%%