import mysql.connector
import sqlalchemy


def sqlcol(dfparam):    
    
    dtypedict = {}
    for i,j in zip(dfparam.columns, dfparam.dtypes):
        # print(i,j)
        if "object" in str(j):
            dtypedict.update({i: sqlalchemy.types.NVARCHAR(length=255)})
                                 
        if "datetime" in str(j)[0:8]:
            dtypedict.update({i: sqlalchemy.types.DateTime()})

        if "float" in str(j):
            dtypedict.update({i: sqlalchemy.types.Float(precision=3, asdecimal=True)})

        if "int" in str(j):
            dtypedict.update({i: sqlalchemy.types.INT()})

    return dtypedict
