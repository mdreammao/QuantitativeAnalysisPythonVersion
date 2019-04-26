import pymssql
import cx_Oracle as oracle
import pandas as pd
from Config.myConstant import *
from Config.myConfig import *
import datetime
import h5py
import os


########################################################################
class IndustryDailyKLineProcess(object):
    """从SqlServer/RDF/本地文件中获取数据"""
    
    #----------------------------------------------------------------------
    def __init__(self,KLineLevel=EMPTY_STRING,update=False,SqlSource=SqlServer['server170'],OracleSource=OracleServer['default']):
        """Constructor"""
        self.KLineLevel=KLineLevel
        self.update=update
        strArry=SqlSource.split(';')
        self.address=strArry[0].split('=')[1]
        self.user=strArry[1].split('=')[1]
        self.password=strArry[2].split('=')[1]
        self.oracleConnectStr=OracleSource
    #----------------------------------------------------------------------
    def __getSqlServerConnectionInfoBystring(self,str):
        strArry=str.split(';')
        self.address=strArry[0].split('=')[1]
        self.user=strArry[1].split('=')[1]
        self.password=strArry[2].split('=')[1]
    #----------------------------------------------------------------------
    #输入code='80XXXX.SI'，startdate=yyyyMMdd，endDate=yyyyMMdd
    def __getDataByDateFromSource(self,code,startDate=EMPTY_STRING,endDate=EMPTY_STRING):
        code=str(code).upper()
        startDate=str(startDate)
        endDate=str(endDate)
        if self.KLineLevel=='minute':
            pass
            #return self.__getMinuteDataByDateFromSqlSever(code,startDate,endDate)
        elif self.KLineLevel=='daily':
            pass
            #return self.__getDailyDataByDateFromOracleServer(code,startDate,endDate)
        
    #----------------------------------------------------------------------
    #输入code=600000.SH，startdate=yyyyMMdd，endDate=yyyyMMdd
    def __saveDataToLocalFile(self,localFileStr,data):
        store = pd.HDFStore(localFileStr,'a')
        if self.KLineLevel=='minute':
            store.append(self.KLineLevel,data,append=True,format="table",data_columns=['code','date','time','open','high','low','close','volume','amount'])
        elif self.KLineLevel=='daily':
            store.append(self.KLineLevel,data,append=True,format="table",data_columns=['code','date','open','high','low','close','preClose','volume','amount','change','pctChange','adjFactor','vwap','status'])
        store.close()

    #----------------------------------------------------------------------
    #输入code=600000.SH，startdate=yyyyMMdd，endDate=yyyyMMdd
    def __getDataByDateFromLocalFile(self,code,startDate,endDate):
        code=str(code).upper();
        localFileStr=LocalFileAddress+"\\{0}\\{1}.h5".format(self.KLineLevel,code.replace('.','_'))
        exists=os.path.isfile(localFileStr)
        if exists==True:
            f=h5py.File(localFileStr,'r')
            myKeys=list(f.keys())
            f.close()
            if myKeys==[]:
                exists=False
        if exists==False:
            mydata=self.__getDataByDateFromSource(code)
            self.__saveDataToLocalFile(localFileStr,mydata)
        else:
            if self.update==True:
                store = pd.HDFStore(localFileStr,'a')
                mydata=store.select(self.KLineLevel)
                store.close()
                if endDate==EMPTY_STRING or mydata['date'].max()<endDate:
                    latestDate=datetime.datetime.strptime(mydata['date'].max(),"%Y%m%d")
                    updateDate=(latestDate+datetime.timedelta(days=1)).strftime("%Y%m%d")
                    updateData=self.__getDailyDataByDateFromOracleServer(code,updateDate)
                    self.__saveDataToLocalFile(localFileStr,updateData)
        store = pd.HDFStore(localFileStr,'a')
        mydata=store.select(self.KLineLevel,where=['date>="%s" and date<="%s"'%(startDate,endDate)])
        store.close()
        return mydata

    

    
    #----------------------------------------------------------------------
    #输入code=600000.SH，startdate=yyyyMMdd，endDate=yyyyMMdd
    def __getDailyDataByDateFromOracleServer(self,code,startDate=EMPTY_STRING,endDate=EMPTY_STRING):
        database='wind_filesync.ASWSIndexEOD'
        connection = oracle.connect(self.oracleConnectStr)
        cursor = connection.cursor()
        oracleStr="select  S_INFO_WINDCODE as code,TRADE_DT as \"date\",S_DQ_OPEN as open,S_DQ_HIGH as high,S_DQ_LOW as low,S_DQ_CLOSE as close,S_DQ_PRECLOSE as preClose,S_DQ_VOLUME as volume,S_DQ_AMOUNT as amount,S_VAL_PE as pe,S_VAL_PB as pb,S_DQ_MV as freeMarketValue,S_VAL_MV as totalMarketValue from wind_filesync.ASWSIndexEOD where S_INFO_WINDCODE='{0}'"
        if startDate==EMPTY_STRING:
            oracleStr=ooracle+"order by TRADE_DT".format(code)
        elif endDate==EMPTY_STRING:
            oracleStr="select  S_INFO_WINDCODE as code,TRADE_DT as \"date\",S_DQ_OPEN as open,S_DQ_HIGH as high,S_DQ_LOW as low,S_DQ_CLOSE as close,S_DQ_PRECLOSE as preClose,S_DQ_VOLUME as volume,S_DQ_AMOUNT as amount,S_DQ_CHANGE as change,S_DQ_PCTCHANGE as pctChange,S_DQ_ADJFACTOR as adjFactor,S_DQ_AVGPRICE as vwap,S_DQ_TRADESTATUS as status from wind_filesync.AShareEODPrices where S_INFO_WINDCODE='{0}' and TRADE_DT>={1} order by TRADE_DT".format(code,startDate)
        else:
            oracleStr="select  S_INFO_WINDCODE as code,TRADE_DT as \"date\",S_DQ_OPEN as open,S_DQ_HIGH as high,S_DQ_LOW as low,S_DQ_CLOSE as close,S_DQ_PRECLOSE as preClose,S_DQ_VOLUME as volume,S_DQ_AMOUNT as amount,S_DQ_CHANGE as change,S_DQ_PCTCHANGE as pctChange,S_DQ_ADJFACTOR as adjFactor,S_DQ_AVGPRICE as vwap,S_DQ_TRADESTATUS as status from wind_filesync.AShareEODPrices where S_INFO_WINDCODE='{0}' and TRADE_DT>={1} and TRADE_DT<={2} order by TRADE_DT".format(code,startDate,endDate)
        cursor.execute(oracleStr)
        mydata = cursor.fetchall()
        mydata = pd.DataFrame(mydata,columns=['code','date','open','high','low','close','preClose','volume','amount','change','pctChange','adjFactor','vwap','status'])
        mydata[['open','high','low','close','preClose','volume','amount','change','pctChange','adjFactor','vwap']] = mydata[['open','high','low','close','preClose','volume','amount','change','pctChange','adjFactor','vwap']].astype('float')
        return mydata  

    #----------------------------------------------------------------------
    #输入code=600000.SH，startdate=yyyyMMdd，endDate=yyyyMMdd
    def __getMinuteDataByDateFromSqlSever(self,code,startDate=EMPTY_STRING,endDate=EMPTY_STRING):
        database='MinuteLine'
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
    def getDataByDate(self,code,startDate,endDate):
        localdata=self.__getDataByDateFromLocalFile(code,str(startDate),str(endDate))
        return localdata
    
    
    
    
     


########################################################################
