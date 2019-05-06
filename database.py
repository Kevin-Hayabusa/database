import pandas as pd
import os
from pybbg import pybbg_k as pybbg
from datetime import datetime,date,time
from pathlib import Path

class Database_hdf5(object):
    '''
    This class is use to download data from bloomberg using pybbg package and store data in HDF5 
    '''
    def __init__(self):
        '''initialize configurations'''


        self.path = r'C:\Users\Kevin\OneDrive - The Chinese University of Hong Kong\Documents\Quant Trading\Database\hdf5_db'
        self.factor_file_name ='factors.h5'
        self.price_file_name = 'prices.h5'
        self.group_name ='HK'
        self.tickers = self.getIndexComposition(r'C:\Users\Kevin\OneDrive - The Chinese University of Hong Kong\Documents\Quant Trading\Database\Index Compositions\HSCI.xlsx')
        self.prices_fields = ['PX_OPEN', 'PX_HIGH', 'PX_LOW', 'PX_LAST', 'PX_VOLUME']
        self.factors_fields = ['HISTORICAL_MARKET_CAP','PX_TO_BOOK_RATIO','PE_RATIO','HIST_CALL_IMP_VOL']
        self.start_date=date(2000,1,1)
        self.end_date=date(2019,1,1)
        
    def getIndexComposition(self,file_name):
        data = pd.read_excel(file_name)
        return list(data['Ticker'])
    
    def download_daily_data(self,tickers,start_date,end_date,fields):
        self.bbg = pybbg.Pybbg()
        self.bbg.service_refData()
        data = self.bbg.bdh(tickers,fields,start_date,end_date, periodselection='DAILY')
        #Check whether it is empty dataframe
        if data.empty:
            print("no data return!")
        else:
            data = data.stack(level=0)
            if len(fields)==1:
                data = data.to_frame()
                data.columns=fields
            elif type(fields)==str:
                data = data.to_frame()
                data.columns=[fields]
            
        data.index.names=['date','ticker']
        data.columns.name='field'
        return data
    def write_price_data(self,price_data,path,file_name,group_name):

        with pd.HDFStore(os.path.join(path,file_name),mode='a') as store:
            tables = dict(list(price_data.groupby(['ticker']))) #store into tables by fields
            for key,values in tables.items():
                df = values.droplevel(level=1)
                #tables[key]=df
                store.put('/'+group_name+'/'+key,df,data_columns=True)
   
    def write_factor_data(self,factor_data,path,file_name,group_name):
        factor_data = factor_data.unstack().stack(level=0)

        with pd.HDFStore(os.path.join(path,file_name),mode='a') as store:
            tables = dict(list(factor_data.groupby(['field']))) #store into tables by fields
            for key,values in tables.items():
                df = values.droplevel(level=1)
                #tables[key]=df
                store.put('/'+group_name+'/'+key,df,data_columns=True)
                #store.put(os.path.join(group_name,key),tables[key],data_columns=True)
            
    def read_factor_data(self,path,file_name,group_name,fields):
        data ={}
        #store = pd.HDFStore(os.path.join(path,file_name))
        with pd.HDFStore(os.path.join(path,file_name),mode='r') as store:
            for fd in fields:
                data[fd] = store.select('/'+group_name+'/'+fd)
        df = pd.concat(data,axis=1)
        df = df.stack(level=1)
        df.columns.name='field'
        #store.close()
        return df
    def get_price_data(self,path,file_name,group_name,tickers,fields='PX_LAST'):
        data ={}
        with pd.HDFStore(os.path.join(path,file_name),mode='r') as store:
            for t in tickers:
                try:
                    data[t] = store.select('/'+group_name+'/'+t)[fields]
                except:
                    print(f"ticker {t} not found")
        
        df = pd.concat(data,axis=1)
        if len(fields)==0 or type(fields)==str:
            df.columns.set_names('ticker',inplace=True)
        else:
            df.columns.set_names('ticker',level=0,inplace=True)
        if(len(tickers)==1):
            df=df.droplevel(0,axis=1)
        return df
    
    def add_tickers(self,new_tickers,path,file_name,group_name,fields):
        '''
        add new tickers with field data and update the specified table in HDF5
        '''
        new_data = self.download_daily_data(new_tickers,self.start_date,self.end_date,fields)
        old_data = self.read_daily_data(path,file_name,group_name,fields)
        merged_data = pd.concat([old_data.unstack().stack(level=0),new_data.unstack().stack(level=0)],axis=1)
        merged_data = merged_data.unstack().stack(level=0)
        merged_data.columns.name = 'field'
        self.write_factor_data(merged_data,path,file_name,group_name)
    def add_fields(self,new_fields,path,file_name,group_name):
        new_data = self.download_daily_data(self.tickers,self.start_date,self.end_date,new_fields)
        self.write_factor_data(new_data,path,file_name,group_name)
        
def updatePrice(db):
    
    price_data = db.download_daily_data(db.tickers,db.start_date,db.end_date,db.prices_fields)
    factor_data = db.download_daily_data(db.tickers,db.start_date,db.end_date,db.factors_fields)
    db.write_price_data(price_data,db.path,db.price_file_name,db.group_name)
    db.write_factor_data(factor_data,db.path,db.factor_file_name,db.group_name)
    
if __name__ == "__main__":
    db = Database_hdf5()
    #updatePrice(db)
    #db.add_tickers(['700 HK Equity'],db.path,db.file_name,db.group_name,['PX_OPEN', 'PX_HIGH'])



