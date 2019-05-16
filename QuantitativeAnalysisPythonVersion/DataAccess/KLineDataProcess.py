import pymssql
import cx_Oracle as oracle
import pandas as pd
from Config.myConstant import *
from Config.myConfig import *
import datetime
import h5py
import os
from DataAccess.TradedayDataProcess import *
from DataAccess.StockIPOInfoProcess import *
from Utility.JobLibUtility import *
from Utility.HDF5Utility import *


########################################################################
class KLineDataProcess(object):
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
    def getSqlServerConnectionInfo(self,address='(local)',user='sa',password='maoheng0',database=EMPTY_STRING,table=EMPTY_STRING):
        self.address=address
        self.user=user
        self.password=password
        self.database=database
        self.table=table

    
    #----------------------------------------------------------------------
    #输入code=600000.SH，startdate=yyyyMMdd，endDate=yyyyMMdd
    def __getDataByDateFromSource(self,code,startDate=EMPTY_STRING,endDate=EMPTY_STRING):
        code=str(code).upper()
        startDate=str(startDate)
        endDate=str(endDate)
        '''
        if startDate==EMPTY_STRING:
            logger.info(f'startDate and endDate are not given,get all data of {code}({self.KLineLevel})')
        elif endDate==EMPTY_STRING:
            logger.info(f'endDate is not given,get all data of {code}({self.KLineLevel}) from {startDate} to end')
        else:
            logger.info(f'get data of {code}({self.KLineLevel}) from {startDate} to {endDate}')
        '''
        if self.KLineLevel=='minute':
            return self.__getMinuteDataByDateFromSqlSever(code,startDate,endDate)
        elif self.KLineLevel=='daily':
            return self.__getDailyDataByDateFromOracleServer(code,startDate,endDate)
        elif self.KLineLevel=='dailyDerivative':
            return self.__getDailyDerivativeDataByDateFromOracleServer(code,startDate,endDate)
        elif self.KLineLevel=='dailyIndex':
            return self.__getDailyIndexDataByDateFromOracleServer(code,startDate,endDate)
        elif self.KLineLevel=='minuteIndex':
            return self.__getMinuteDataByDateFromSqlSever(code,startDate,endDate)
        
    #----------------------------------------------------------------------
    #输入code=600000.SH，startdate=yyyyMMdd，endDate=yyyyMMdd
    def __saveDataToLocalFile(self,localFileStr,data):
        if data.shape[0]==0:
            return
        mydate=data['date'].drop_duplicates()
        try:
            store = pd.HDFStore(localFileStr,'a',complib='blosc:zstd',append=True,complevel=9)
            store.append(self.KLineLevel,data,append=True,format="table",data_columns=data.columns)
            store.append('date',mydate,append=True,format="table",data_columns=['date'])
            store.close()
        except:
            logger.error(f'{localFileStr} error!')
        

    #----------------------------------------------------------------------
    #输入code=600000.SH，startdate=yyyyMMdd，endDate=yyyyMMdd
    def __getDataByDateFromLocalFile(self,code,startDate,endDate):
         #如果股票已经退市，需要对endDate做一些调整
        IPOInfo=StockIPOInfoProcess.getStockIPOInfoByCode(code)
        if IPOInfo.empty==False:
            listDate=IPOInfo['listDate'].iloc[0]
            delistDate=IPOInfo['delistDate'].iloc[0]
            if startDate<listDate:
                startDate=listDate
                pass
            if endDate>=delistDate:
                endDate=TradedayDataProcess.getPreviousTradeday(delistDate)
        if startDate<=endDate:
            logger.info(f'get data of {code} from {startDate} to {endDate} start!')
        else:
            logger.warning(f'There is no data of {code} from {startDate} to {endDate}')
            mydata=pd.DataFrame()
            return mydata
        if self.update==True:
            mydata=self.__getDataByDateFromLocalFileWithUpdate(code,startDate,endDate)
        else:
            mydata=self.__getDataByDateFromLocalFileWithoutUpdate(code,startDate,endDate)
        return mydata
        pass
    #----------------------------------------------------------------------
    #输入code=600000.SH，startdate=yyyyMMdd，endDate=yyyyMMdd
    def __getDataByDateFromLocalFileWithoutUpdate(self,code,startDate,endDate):
        code=str(code).upper();
        #logger.info(f'get data of {code}({self.KLineLevel}) start!')
        fileName=code.replace('.','_')+".h5"
        localFileStr=os.path.join(LocalFileAddress,'KLines',self.KLineLevel,fileName)
        exists=os.path.isfile(localFileStr)
        mydata=pd.DataFrame()
        if exists==True:
            f=h5py.File(localFileStr,'r')
            myKeys=list(f.keys())
            f.close()
            if myKeys==[]:
                logger.warning(f'{localFileStr} has no data!{localFileStr} will be deleted!')
                os.remove(localFileStr)
                exists=False
        if exists==False:
            logger.error(f'{code} has no data!')
        else:
            store = pd.HDFStore(localFileStr,'a',complib='blosc:zstd',append=True,complevel=9)
            mydata=store.get(self.KLineLevel)
            # mydata=store.select(self.KLineLevel,where=['date>="%s" and date<="%s"'%(startDate,endDate)])
            mydata=mydata[(mydata['date']>=startDate) & (mydata['date']<=endDate)]
            store.close()
            #logger.info(f'get data of {code}({self.KLineLevel}) compelete!')
        return mydata
    #----------------------------------------------------------------------
    #输入code=600000.SH，startdate=yyyyMMdd，endDate=yyyyMMdd
    def __getDataByDateFromLocalFileWithUpdate(self,code,startDate,endDate):
        code=str(code).upper();
        logger.info(f'get data of {code}({self.KLineLevel}) start!')
        #localFileStr=LocalFileAddress+"\\KLines\\{0}\\{1}.h5".format(self.KLineLevel,code.replace('.','_'))
        fileName=code.replace('.','_')+".h5"
        localFilePath=os.path.join(LocalFileAddress,'KLines',self.KLineLevel)
        HDF5Utility.pathCreate(localFilePath)
        localFileStr=os.path.join(LocalFileAddress,'KLines',self.KLineLevel,fileName)
        exists=os.path.isfile(localFileStr)
        if exists==True:
            f=h5py.File(localFileStr,'r')
            myKeys=list(f.keys())
            f.close()
            if myKeys==[]:
                logger.warning(f'{localFileStr} has no data!{localFileStr} will be deleted!')
                os.remove(localFileStr)
                exists=False
        if exists==False:
            mydata=self.__getDataByDateFromSource(code)
            if mydata.empty==False:
                self.__saveDataToLocalFile(localFileStr,mydata)
                exists=True
            else:
                logger.error(f'{localFileStr} has no data from source!')
        else:
            if self.update==True:
                try:
                    store = pd.HDFStore(localFileStr,'a',complib='blosc:zstd',append=True,complevel=9)
                    #mydata=store.select(self.KLineLevel)
                    #mydate=mydata['date'].drop_duplicates()
                    #store.append('date',mydate,append=False,format="table",data_columns=['date'])
                    mydate=store['date']
                    store.close()
                except :
                    logger.error(f'read data({self.KLineLevel}) error of {code}')
                    return pd.DataFrame()
                    pass
                if endDate==EMPTY_STRING or mydate.max()<endDate:
                    #更新日期从后一个交易日开始
                    updateDate=TradedayDataProcess.getNextTradeday(mydate.max())
                    yesterday=(datetime.datetime.now()+datetime.timedelta(days=-1)).strftime("%Y%m%d")
                    #更新到昨日或者指定的日期
                    if (endDate>yesterday) | (endDate==EMPTY_STRING):
                        endDate=yesterday
                    if endDate>=updateDate:
                        updateData=self.__getDataByDateFromSource(code,updateDate,endDate)
                        if updateData.empty==False:
                            self.__saveDataToLocalFile(localFileStr,updateData)
        if exists==False:
            mydata=pd.DataFrame()
        else:
            store = pd.HDFStore(localFileStr,'a',complib='blosc:zstd',append=True,complevel=9)
            mydata=store.get(self.KLineLevel)
            # mydata=store.select(self.KLineLevel,where=['date>="%s" and date<="%s"'%(startDate,endDate)])
            mydata=mydata[(mydata['date']>=startDate) & (mydata['date']<=endDate)]
            store.close()
        #mydata.set_index('date',drop=True,inplace=True)
        return mydata

    
    #----------------------------------------------------------------------
    #输入code=600000.SH，startdate=yyyyMMdd，endDate=yyyyMMdd
    def __getDailyDerivativeDataByDateFromOracleServer(self,code,startDate=EMPTY_STRING,endDate=EMPTY_STRING):
        #获取衍生数据
        #1表示涨停；0表示非涨停或跌停；-1表示跌停。
        database='wind_filesync.AShareEODDerivativeIndicator'
        connection = oracle.connect(self.oracleConnectStr)
        cursor = connection.cursor()
        oracleStr="select  S_INFO_WINDCODE as code,TRADE_DT as \"date\", S_VAL_MV as totalMarketValue,S_DQ_MV as freeMarketValue,S_VAL_PE_TTM as PE,S_VAL_PCF_NCFTTM as PCF,S_VAL_PS_TTM as PS,S_DQ_FREETURNOVER as turnover,TOT_SHR_TODAY as totalShares,FREE_SHARES_TODAY as freeShares,UP_DOWN_LIMIT_STATUS as limitStatus from wind_filesync.AShareEODDerivativeIndicator "
        if startDate==EMPTY_STRING:
            oracleStr=oracleStr+"where S_INFO_WINDCODE='{0}' order by TRADE_DT".format(code)
        elif endDate==EMPTY_STRING:
            oracleStr=oracleStr+"where S_INFO_WINDCODE='{0}' and TRADE_DT>={1} order by TRADE_DT".format(code,startDate)
        else:
            oracleStr=oracleStr+"where S_INFO_WINDCODE='{0}' and TRADE_DT>={1} and TRADE_DT<={2} order by TRADE_DT".format(code,startDate,endDate)
        cursor.execute(oracleStr)
        myderivativedata = cursor.fetchall()
        myderivativedata = pd.DataFrame(myderivativedata,columns=['code','date','totalMarketValue','freeMarketValue','PE','PCF','PS','turnover','totalShares','freeShares','limitStatus'])
        if (myderivativedata.shape[0]==0):
            return myderivativedata
        mytradedays=TradedayDataProcess.getAllTradedays()
        myderivativedata=myderivativedata[myderivativedata['date'].isin(mytradedays)]
        myderivativedata[['totalMarketValue','freeMarketValue','PE','PCF','PS','turnover','totalShares','freeShares','limitStatus']] = myderivativedata[['totalMarketValue','freeMarketValue','PE','PCF','PS','turnover','totalShares','freeShares','limitStatus']].astype('float')

        return myderivativedata

    
    #----------------------------------------------------------------------
    #输入code=600000.SH，startdate=yyyyMMdd，endDate=yyyyMMdd
    def __getDailyDataByDateFromOracleServer(self,code,startDate=EMPTY_STRING,endDate=EMPTY_STRING):
        #获取行情数据
        #附注status -1:交易-2:待核查0:停牌XD:除息XR:除权DR:除权除息N:上市首日
        database='wind_filesync.AShareEODPrices'
        connection = oracle.connect(self.oracleConnectStr)
        cursor = connection.cursor()
        if startDate==EMPTY_STRING:
            oracleStr="select  S_INFO_WINDCODE as code,TRADE_DT as \"date\",S_DQ_OPEN as open,S_DQ_HIGH as high,S_DQ_LOW as low,S_DQ_CLOSE as close,S_DQ_PRECLOSE as preClose,S_DQ_VOLUME as volume,S_DQ_AMOUNT as amount,S_DQ_CHANGE as change,S_DQ_PCTCHANGE as pctChange,S_DQ_ADJFACTOR as adjFactor,S_DQ_AVGPRICE as vwap,S_DQ_TRADESTATUS as status from wind_filesync.AShareEODPrices where S_INFO_WINDCODE='{0}' order by TRADE_DT".format(code)
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
    def __getDailyIndexDataByDateFromOracleServer(self,code,startDate=EMPTY_STRING,endDate=EMPTY_STRING):
        #获取行情数据
        #附注status -1:交易-2:待核查0:停牌XD:除息XR:除权DR:除权除息N:上市首日
        database='wind_filesync.AIndexEODPrices'
        connection = oracle.connect(self.oracleConnectStr)
        cursor = connection.cursor()
        oracleStr="select  S_INFO_WINDCODE as code,TRADE_DT as \"date\",S_DQ_OPEN as open,S_DQ_HIGH as high,S_DQ_LOW as low,S_DQ_CLOSE as close,S_DQ_PRECLOSE as preClose,S_DQ_VOLUME as volume,S_DQ_AMOUNT as amount,S_DQ_CHANGE as change,S_DQ_PCTCHANGE as pctChange from wind_filesync.AIndexEODPrices "
        if startDate==EMPTY_STRING:
            oracleStr=oracleStr+"where S_INFO_WINDCODE='{0}' order by TRADE_DT".format(code)
        elif endDate==EMPTY_STRING:
            oracleStr=oracleStr+"where S_INFO_WINDCODE='{0}' and TRADE_DT>={1} order by TRADE_DT".format(code,startDate)
        else:
            oracleStr=oracleStr+"where S_INFO_WINDCODE='{0}' and TRADE_DT>={1} and TRADE_DT<={2} order by TRADE_DT".format(code,startDate,endDate)
        cursor.execute(oracleStr)
        mydata = cursor.fetchall()
        mydata = pd.DataFrame(mydata,columns=['code','date','open','high','low','close','preClose','volume','amount','change','pctChange'])
        mydata[['open','high','low','close','preClose','volume','amount','change','pctChange']] = mydata[['open','high','low','close','preClose','volume','amount','change','pctChange']].astype('float')
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
    #----------------------------------------------------------------------
    #输入code=600000.SH，startdate=yyyyMMdd，endDate=yyyyMMdd
    def getLotsDataByDate(self,StockCodes,startDate,endDate):
        mydata=pd.DataFrame()
        for i in range(len(StockCodes)):
            code=StockCodes[i]
            #print(code)
            localdata=self.__getDataByDateFromLocalFile(code,str(startDate),str(endDate))
            mydata=mydata.append(localdata)
        return mydata
    #----------------------------------------------------------------------
    #输入code=600000.SH，startdate=yyyyMMdd，endDate=yyyyMMdd
    def updateLotsDataByDate(self,StockCodes,startDate,endDate):
        mydata=pd.DataFrame()
        for i in range(len(StockCodes)):
            code=StockCodes[i]
            self.__getDataByDateFromLocalFile(code,str(startDate),str(endDate))
           
    #----------------------------------------------------------------------
    def parallelizationGetDataByDate(self,stockCodes,startDate,endDate):
        mydata=JobLibUtility.useJobLibToGetData(self.getLotsDataByDate,stockCodes,MYGROUPS,startDate,endDate)
        return mydata
        pass
    #----------------------------------------------------------------------
    def parallelizationUpdateDataByDate(self,stockCodes,startDate,endDate):
        JobLibUtility.useJobLibToUpdateData(self.updateLotsDataByDate,stockCodes,MYGROUPS,startDate,endDate)
        pass
    
    
    
     


########################################################################

