import pymssql
import cx_Oracle as oracle
import pandas as pd
from Config.myConstant import *
from Config.myConfig import *
import datetime
import h5py
import os
from DataAccess.TradedayDataProcess import *
from Utility.JobLibUtility import *
from Utility.HDF5Utility import *

########################################################################
class TickDataProcess(object):
    """从170数据库获取TICK数据"""
    #----------------------------------------------------------------------
    def __init__(self,tickLevel=EMPTY_STRING,update=False,SqlSource=SqlServer['server170']):
        """Constructor"""
        self.tickLevel=tickLevel
        self.update=update
    #----------------------------------------------------------------------
    #输入code=600000.SH，startdate=yyyyMMdd，endDate=yyyyMMdd
    def getDataByDate(self,code,startDate,endDate):
        localdata=self.__getDataByDateFromLocalFile(code,str(startDate),str(endDate))
        return localdata
    #----------------------------------------------------------------------
    #输入code=600000.SH，startdate=yyyyMMdd，endDate=yyyyMMdd
    def __getDataByDateFromSource(self,code,startDate=EMPTY_STRING,endDate=EMPTY_STRING):
        code=str(code).upper()
        startDate=str(startDate)
        endDate=str(endDate)
        if self.tickLevel=='stockTickShot':
            return self.__getStockTickShotDataByDateFromSqlSever(code,startDate,endDate)
    #----------------------------------------------------------------------
    #输入code=600000.SH，startdate=yyyyMMdd，endDate=yyyyMMdd
    def __saveDataToLocalFile(self,localFileStr,data):
        if data.shape[0]==0:
            return
        store = pd.HDFStore(localFileStr,'a',complib='blosc:zstd',append=True,complevel=9)
        if self.tickLevel=='stockTickShot':
            store.append(self.tickLevel,data,append=True,format="table",data_columns=data.columns)
        store.close()
    #----------------------------------------------------------------------
    #输入code=600000.SH，startdate=yyyyMMdd，endDate=yyyyMMdd
    def __getStockTickShotDataByDateFromSqlSever(self,code,startDate=EMPTY_STRING,endDate=EMPTY_STRING):
        database='MarketData'
        connect=pymssql.connect( self.address,self.user,self.password,database,charset='utf8')
        cursor = connect.cursor()
        if startDate==EMPTY_STRING:
             sql = "SELECT [stkcd] as [code], [tdate] as [date],[ttime] as [time],[Open] as [open] ,[High] as [high],[Low] as [low],[Close] as [close] ,[Volume] as [volume],[Amount] as [amount]  FROM [{0}].[dbo].[Min1_{1}] order by [tdate],[ttime]".format(database,code.replace('.','_'))
        elif endDate==EMPTY_STRING:
            sql = "SELECT [stkcd] as [code], [tdate] as [date],[ttime] as [time],[Open] as [open] ,[High] as [high],[Low] as [low],[Close] as [close] ,[Volume] as [volume],[Amount] as [amount]  FROM [{0}].[dbo].[Min1_{1}] where [tdate]>={2} order by [tdate],[ttime]".format(database,code.replace('.','_'),startDate)
        else:
            sql = "SELECT [stkcd] as [code], [tdate] as [date],[ttime] as [time],[Open] as [open] ,[High] as [high],[Low] as [low],[Close] as [close] ,[Volume] as [volume],[Amount] as [amount]  FROM [{0}].[dbo].[Min1_{1}] where [tdate]>={2} and [tdate]<={3} order by [tdate],[ttime]".format(database,code.replace('.','_'),startDate,endDate)
            
        cursor.execute(sql)
        mydata=cursor.fetchall()
        mydata = pd.DataFrame(mydata,columns=['code' ,'date','time' ,'open' ,'high','low','close' ,'volume' ,'amount'])
        mydata[['open','high','low','close','volume','amount']] = mydata[['open','high','low','close','volume','amount']].astype('float')
        return mydata    

    #----------------------------------------------------------------------
    #输入code=600000.SH，startdate=yyyyMMdd，endDate=yyyyMMdd
    def __getDataByDateFromLocalFile(self,code,startDate,endDate):
        code=str(code).upper();
        fileName=code.replace('.','_')+".h5"
        localFilePath=os.path.join(LocalFileAddress,'Ticks',self.tickLevel)
        HDF5Utility.pathCreate(localFilePath)
        localFileStr=os.path.join(LocalFileAddress,'Ticks',self.tickLevel,fileName)
        exists=os.path.isfile(localFileStr)
        if exists==True:
            f=h5py.File(localFileStr,'r')
            myKeys=list(f.keys())
            f.close()
            if myKeys==[]:
                exists=False
        if exists==False:
            mydata=self.__getDataByDateFromSource(code)
            os.remove(localFileStr)
            self.__saveDataToLocalFile(localFileStr,mydata)
        else:
            if self.update==True:
                store = pd.HDFStore(localFileStr,'a',complib='blosc:zstd',append=True,complevel=9)
                mydata=store.select(self.tickLevel)
                store.close()
                if endDate==EMPTY_STRING or mydata['date'].max()<endDate:
                    latestDate=datetime.datetime.strptime(mydata['date'].max(),"%Y%m%d")
                    updateDate=(latestDate+datetime.timedelta(days=1)).strftime("%Y%m%d")
                    updateData=self.__getDataByDateFromSource(code,updateDate)
                    self.__saveDataToLocalFile(localFileStr,updateData)
        
        f=h5py.File(localFileStr,'r')
        myKeys=list(f.keys())
        f.close()
        if myKeys==[]:
            mydata=pd.DataFrame()
        else:
            store = pd.HDFStore(localFileStr,'a',complib='blosc:zstd',append=True,complevel=9)
            mydata=store.get(self.tickLevel)
            # mydata=store.select(self.KLineLevel,where=['date>="%s" and date<="%s"'%(startDate,endDate)])
            mydata=mydata[(mydata['date']>=startDate) & (mydata['date']<=endDate)]
            store.close()
        #mydata.set_index('date',drop=True,inplace=True)
        return mydata