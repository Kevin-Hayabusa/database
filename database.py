import pandas as pd
import os
from pybbg import pybbg_k as pybbg
from datetime import datetime,date,time

class Database_hdf5(object):
    '''
    This class is use to download data from bloomberg using pybbg package and store data in HDF5 
    '''
    def __init__(self):
        '''initialize configurations'''

        self.bbg = pybbg.Pybbg()
        self.bbg.service_refData()
        self.path = r'C:\Users\Kevin\OneDrive - The Chinese University of Hong Kong\Documents\Quant Trading\Database\hdf5_db'
        self.file_name ='price.h5'
        self.group_name ='HK'
        self.tickers = ['5 HK Equity','1 HK Equity']
        self.price_fields = ['PX_OPEN', 'PX_HIGH', 'PX_LOW', 'PX_LAST', 'PX_VOLUME']
        self.start_date=date(2018,1,1)
        self.end_date=date(2019,1,1)
        
    def download_daily_data(self,tickers,start_date,end_date,fields):

        data = self.bbg.bdh(tickers,fields,start_date,end_date, periodselection='DAILY')
        data.index.name='date'
        data = data.stack(level=0)
        return data
    def write_daily_data(self,daily_data,path,file_name,group_name):
        daily_data = daily_data.unstack().stack(level=0)
        with pd.HDFStore(os.path.join(path,file_name),mode='a') as store:
        
            tables = dict(list(daily_data.groupby(['field']))) #store into tables by fields
            for key,values in tables.items():
                df = values.droplevel(level=1)
                #tables[key]=df
                store.put('/'+group_name+'/'+key,df,data_columns=True)
                #store.put(os.path.join(group_name,key),tables[key],data_columns=True)
            
    def read_daily_data(self,path,file_name,group_name,fields):
        data ={}
        #store = pd.HDFStore(os.path.join(path,file_name))
        with pd.HDFStore(os.path.join(path,file_name),mode='r') as store:
            for fd in fields:
                data[fd] = store.select('/'+group_name+'/'+fd)
        df = pd.concat(data,axis=1)
        #store.close()
        return df.stack(level=1)
    def add_tickers(self,new_tickers,path,file_name,group_name,fields):
        '''
        add new tickers with field data and update the specified table in HDF5
        '''
        new_data = self.download_daily_data(new_tickers,self.start_date,self.end_date,fields)
        old_data = self.read_daily_data(path,file_name,group_name,fields)
        merged_data = pd.concat([old_data.unstack().stack(level=0),new_data.unstack().stack(level=0)],axis=1)
        merged_data = merged_data.unstack().stack(level=0)
        merged_data.columns.name = 'field'
        self.write_daily_data(merged_data,path,file_name,group_name)
        
def updatePrice(db):
    
    price_data = db.download_daily_data(db.tickers,db.start_date,db.end_date,db.price_fields)
    db.write_daily_data(price_data,db.path,db.file_name,db.group_name)
    
if __name__ == "__main__":
    db = Database_hdf5()
    updatePrice(db)
    db.add_tickers(['700 HK Equity'],db.path,db.file_name,db.group_name,['PX_OPEN', 'PX_HIGH'])



