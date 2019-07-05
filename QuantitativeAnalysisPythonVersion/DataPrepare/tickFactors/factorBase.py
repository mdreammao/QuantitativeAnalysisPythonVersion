import os
from Config.myConstant import *
from Config.myConfig import *
from Utility.HDF5Utility import HDF5Utility
from DataAccess.TickDataProcess import TickDataProcess
from DataAccess.TradedayDataProcess import TradedayDataProcess
import influxdb
import pandas as pd

########################################################################
class factorBase(object):
    """tick因子的基类"""
    #----------------------------------------------------------------------
    def __init__(self):
        pass
    #----------------------------------------------------------------------
    def clearLocalFile(self,code,date,factor):
        path=os.path.join(LocalFileAddress,'tickFactors',str(factor),str(code))
        fileName=os.path.join(path,str(date)+'.h5')
        HDF5Utility.pathCreate(path)
        exists=os.path.exists(fileName)
        if exists==True:
            os.remove(fileName)
        pass
    def checkLocalFile(self,code,date,factor):
        path=os.path.join(LocalFileAddress,'tickFactors',str(factor),str(code))
        fileName=os.path.join(path,str(date)+'.h5')
        #HDF5Utility.pathCreate(path)
        exists=os.path.exists(fileName)
        return exists
    #----------------------------------------------------------------------
    def updateFactor(self,code,date,factor,data):
        result=data
        if result.shape[0]==0:
            logger.warning(f'There no data of {code} in {date} of factor:{factor}!')
            pass
        else:
            self.saveToLocalFile(code,date,factor,result)
        pass
    #----------------------------------------------------------------------
    def saveToLocalFile(self,code,date,factor,data):
        path=os.path.join(LocalFileAddress,'tickFactors',str(factor),str(code))
        fileName=os.path.join(path,str(date)+'.h5')
        HDF5Utility.pathCreate(path)
        exists=os.path.exists(fileName)
        if exists==False:
            with pd.HDFStore(fileName,'a',complib='blosc:zstd',append=True,complevel=9) as store:
                store.append('data',data,append=False,format="table",data_columns=data.columns)
        pass
    #----------------------------------------------------------------------
    def getDataFromLocalFile(self,code,date,factor):
        path=os.path.join(LocalFileAddress,'tickFactors',str(factor),str(code))
        fileName=os.path.join(path,str(date)+'.h5')
        data=pd.DataFrame()
        exists=os.path.exists(fileName)
        if exists==False:
            logger.warning(f'There is no data of {code}({factor}) in {date} from local file!')
            return data
        with pd.HDFStore(fileName,'r',complib='blosc:zstd',append=True,complevel=9) as store:
            data=store['data']
        return data
        pass
    #----------------------------------------------------------------------
    def getTimeSeriesQuantile(self,mycolumns,data,periods=50,min_periods=20):
        for col in mycolumns:
            data['ts_'+col]=data[col].rolling(periods,min_periods=min_periods).apply((lambda x:pd.Series(x).rank().iloc[-1]/len(x)),raw=True)
        pass
    #----------------------------------------------------------------------
    #check函数，检查因子里面是否存在nan
    def checkDataNan(self,code,date,factor,data):
        errorData=data[data.isna().sum(axis=1)>0]
        if errorData.shape[0]>0:
            logger.warning(f'{factor} of {code} in {date} has nan data!!!')
            return False
        else:
            return True
    #----------------------------------------------------------------------
    def logBetweenTwoColumns(self,data,col1,col2):
        result=data[[col1,col2]].copy()
        result['mylog']=0
        select=(result[col1]>0) & (result[col2]>0)
        result.loc[select,'mylog']=(np.log(result[col1])-np.log(result[col2]))[select]
        return result['mylog']
        pass 
    #----------------------------------------------------------------------
    def EMA(self,series,span):
        return pd.Series.ewm(series, span=span).mean()
        pass
    #----------------------------------------------------------------------
    def MA(self,series,span):
        return series.rolling(span,min_periods=1).mean()
        pass
    #----------------------------------------------------------------------
    def getLastTradedayTickData(self,code,date):
        previousday=TradedayDataProcess.getPreviousTradeday(date,250)
        days=list(TradedayDataProcess.getTradedays(previousday,date))
        days.reverse()
        data=pd.DataFrame()
        for day in days:
            if day<date:
                #data=TickDataProcess().getDataByDateFromLocalFile(code,day)
                data=TickDataProcess().getTickShotDataFromInfluxdbServer(code,day)
                if data.shape[0]>0:
                    return data
                    pass
                pass
            pass
        return data
        pass
    #----------------------------------------------------------------------
    def updateFactorToInfluxdb(self,code,date,factor,data):
        result=data
        if result.shape[0]==0:
            logger.warning(f'There no data of {code} in {date} of factor:{factor}!')
            pass
        else:
            self.saveToInfluxdb(code,date,factor,result)
        pass
    #----------------------------------------------------------------------
    def saveToInfluxdb(self,code,date,factor,data):
        host=InfluxdbServer['host']
        port=InfluxdbServer['port']
        username=InfluxdbServer['username']
        password=InfluxdbServer['password']
        database='MaoTickFactors'
        client = influxdb.DataFrameClient(host=host, port=port, username=username, password=password, database=database)
        dbs = client.get_list_database()
        if ({'name':database} in dbs)==False:
            client.create_database(database)
        client.write_points(dataframe=data,
                    database=database,
                    measurement=code,
                    tags={},
                    field_columns=list(data.columns),
                    protocol='line')
        pass
########################################################################
