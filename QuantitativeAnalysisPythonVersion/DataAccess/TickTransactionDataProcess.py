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
from DataAccess.KLineDataProcess import *
from Utility.JobLibUtility import *
from Utility.HDF5Utility import *
import pymssql

########################################################################
class TickTransactionDataProcess(object):
    """从170数据库获取TICK数据"""
    #----------------------------------------------------------------------
    def __init__(self,SqlSource=SqlServer['server170']):
        """Constructor"""
        strArry=SqlSource.split(';')
        self.address=strArry[0].split('=')[1]
        self.user=strArry[1].split('=')[1]
        self.password=strArry[2].split('=')[1]
        self.filePath=os.path.join(LocalFileAddress,'TickTransaction')
    #----------------------------------------------------------------------
    def getDataByDateFromLocalFile(self,code,date):
        date=str(date)
        path=os.path.join(self.filePath,code.replace('.','_'))
        file=os.path.join(path,date+'.h5')
        HDF5Utility.pathCreate(path)
        exists=os.path.isfile(file)
        if exists==False:
            logger.warning(f'There is no TickTransaction data of {code} in {date} from local file!')
            mydata=pd.DataFrame()
        else:
            #logger.info(f'get TickTransaction data of {code} in {date} from local file!')
            with pd.HDFStore(file,'r',complib='blosc:zstd',append=True,complevel=9) as store:
                #store = pd.HDFStore(file,'r',complib='blosc:zstd',append=True,complevel=9)
                mydata=store['data']
                #store.close()
            pass
        return mydata
    #----------------------------------------------------------------------
    def recordResampleTickTransactionDataFromSqlServer(self,code,startDate,endDate):
        #获取股票日线数据，筛选出非停牌的日期
        daily=KLineDataProcess('daily')
        dailyData=daily.getDataByDate(code,startDate,endDate)
        if dailyData.empty==True:
            logger.warning(f'There no daily data of {code} from {startDate} to {endDate}!')
            return 
        dailyData=dailyData[dailyData['status']!='停牌']
        tradedays=list(dailyData['date'])
        for date in tradedays:
            path=os.path.join(self.filePath,code.replace('.','_'))
            file=os.path.join(path,date+'.h5')
            HDF5Utility.pathCreate(path)
            exists=os.path.isfile(file)
            if exists==False:
                logger.info(f'get TickTransaction data of {code} in {date} from source!')
                mydata=self.__getResampleTickTransactionDataFromSqlServer(code,date)
                if mydata.empty==False:
                    with pd.HDFStore(file,'a',complib='blosc:zstd',append=True,complevel=9) as store:
                        store.append('data',mydata,append=False,format="table",data_columns=mydata.columns)
                else:
                    logger.warning(f'there is no data of {code} in {date} from source!')
            else:
                logger.info(f'TickTransaction data of {code} in {date} is exists!')
                pass
        pass
    #----------------------------------------------------------------------
    def __getResampleTickTransactionDataFromSqlServer(self,code,date):
        try:
            mydata=self.__getTickTransactionDataFromSqlServer(code,date)
        except:
            logger.error(f'There is no tick data of {code} in {date} from source!!')
            return pd.DataFrame()
            pass
        mydata=mydata[((mydata.index.time>=datetime.time(9,30)) & (mydata.index.time<=datetime.time(11,30))) | ((mydata.index.time>=datetime.time(13,00)) & (mydata.index.time<=datetime.time(15,00)))]
        return mydata
        pass
    #----------------------------------------------------------------------
    def __getTickTransactionDataFromSqlServer(self,code,date):
        date=str(date)
        month=date[0:6]
        database='WindFullMarket'+month
        table='L2_Transaction_'+code.replace('.','_')
        connect=pymssql.connect( self.address,self.user,self.password,database,charset='utf8')
        cursor = connect.cursor()
        sql="select [stkcd],rtrim([tradeDate]),rtrim([tradeTime]),[tradeIndex],[tradePrice],[tradeVolume] FROM [{0}].[dbo].[{1}] where [tradeDate]={2} and ((left(rtrim(tradeTime),6)>=91500 and left(rtrim(tradeTime),6)<=113000) or (left(rtrim(tradeTime),6)>=130000 and left(rtrim(tradeTime),6)<=150000)) order by tradeTime".format(database,table,date)
        cursor.execute(sql)
        mydata=cursor.fetchall()
        mydata = pd.DataFrame(mydata,columns=['code' ,'date','time' ,'tradeIndex','tradePrice','tradeVolume'])
        mydata[['tradePrice']] = mydata[['tradePrice']].astype('float')
        mydata[['tradeIndex','tradeVolume']] = mydata[['tradeIndex','tradeVolume']].astype('int')
        mydata['mytime']=pd.to_datetime(mydata['date']+mydata['time'],format='%Y%m%d%H%M%S%f')
        mydata.set_index('mytime',inplace=True,drop=True)
        return mydata    
        pass
    #----------------------------------------------------------------------
    #输入code=600000.SH，startdate=yyyyMMdd，endDate=yyyyMMdd
    def getLotsDataByDate(self,StockCodes,startDate,endDate):
        mydata=pd.DataFrame()
        for i in range(len(StockCodes)):
            code=StockCodes[i]
            #print(code)
            localdata=self.getDataByDateFromLocalFile(code,str(startDate),str(endDate))
            mydata=mydata.append(localdata)
        return mydata
    #----------------------------------------------------------------------
    #输入code=600000.SH，startdate=yyyyMMdd，endDate=yyyyMMdd
    def updateLotsDataByDate(self,StockCodes,startDate,endDate):
        mydata=pd.DataFrame()
        for i in range(len(StockCodes)):
            code=StockCodes[i]
            self.recordResampleTickTransactionDataFromSqlServer(code,str(startDate),str(endDate))
           
    #----------------------------------------------------------------------
    def parallelizationGetDataByDate(self,stockCodes,startDate,endDate):
        mydata=JobLibUtility.useJobLibToGetData(self.getLotsDataByDate,stockCodes,MYGROUPS,startDate,endDate)
        return mydata
        pass
    #----------------------------------------------------------------------
    def parallelizationUpdateDataByDate(self,stockCodes,startDate,endDate):
        JobLibUtility.useJobLibToUpdateData(self.updateLotsDataByDate,stockCodes,MYGROUPS,startDate,endDate)
        pass