import pandas as pd
import os
from pybbg import pybbg_k as pybbg
from datetime import datetime,date,time

class Database_hdf5(object):

    def __init__(self):

        self.bbg = pybbg.Pybbg()
        self.bbg.service_refData()
        self.path = r'C:\Users\Kevin\OneDrive - The Chinese University of Hong Kong\Documents\Quant Trading\Database\hdf5_db'
        self.table_daily ='data_daily.h5'
    def download_trading_data(self,tickers,start_date,end_date,fields):
        self.tickers = tickers
        self.start_date = start_date.strftime('%Y%m%d')
        self.end_date = end_date.strftime('%Y%m%d')
        self.fields = fields
        price = self.bbg.bdh(self.tickers, self.fields, self.start_date, self.end_date, periodselection='DAILY')
        price = price.stack(level=1)
        price.reset_index(inplace=True)
        price.columns.values[0]='date'
        return price
    def write_trading_data(self,trading_data,path,name,group):
        store = pd.HDFStore(os.path.join(path,name))
        
        tables = dict(list(trading_data.groupby(['field']))) #store into tables by fields
        for key,values in tables.items():
            df = values.drop(columns=['field'],axis=1)
            df.set_index('date',inplace=True)
            tables[key]=df
            store.put(os.path.join(group,key),tables[key],data_columns=True)
        store.close()
    def read_trading_data(self,path,file_name,group_name,fields):
        data ={}
        for fd in fields:
            data[fd] = pd.read_hdf(os.path.join(path,file_name),os.path.join(group_name,fd))
        df = pd.concat(data,axis=1)
        return df.stack(level=1)
def updateDB(db):
    
    tickers = ['5 HK Equity','1 HK Equity']
    trading_fields = ['PX_OPEN', 'PX_HIGH', 'PX_LOW', 'PX_LAST', 'PX_VOLUME']
    trading_data = db.download_trading_data(tickers,date(2018,1,1),date(2019,1,1),trading_fields)
    db.write_trading_data(trading_data,db.path,db.table_daily,'HK')
    
if __name__ == "__main__":
    db = Database_hdf5()
    updateDB(db)



