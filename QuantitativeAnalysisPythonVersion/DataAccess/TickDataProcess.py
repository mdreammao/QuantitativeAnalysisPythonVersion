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
class TickDataProcess(object):
    """从170数据库获取TICK数据"""
    #----------------------------------------------------------------------
    def __init__(self,SqlSource=SqlServer['server170']):
        """Constructor"""
        strArry=SqlSource.split(';')
        self.address=strArry[0].split('=')[1]
        self.user=strArry[1].split('=')[1]
        self.password=strArry[2].split('=')[1]
        self.filePath=os.path.join(LocalFileAddress,'TickShots')
    #----------------------------------------------------------------------
    def getDataByDateFromLocalFile(self,code,startDate,endDate):
        path=os.path.join(self.filePath,code.replace('.','_'))
        file=os.path.join(path,date+'.h5')
        HDF5Utility.pathCreate(path)
        exists=os.path.isfile(file)
        if exists==False:
            logger.warning(f'There is no tickshot data of {code} in {date} from local file!')
            mydata=pd.DataFrame()
        else:
            logger.info(f'get tickshot data of {code} in {date} from local file!')
            store = pd.HDFStore(localFileStr,'a',complib='blosc:zstd',append=True,complevel=9)
            mydata=store['data']
            store.close()
            pass
        return mydata
    #----------------------------------------------------------------------
    def recordResampleTickShotDataFromSqlServer(self,code,startDate,endDate):
        #获取股票日线数据，筛选出非停牌的日期
        daily=KLineDataProcess('daily')
        dailyData=daily.getDataByDate(code,startDate,endDate)
        dailyData=dailyData[dailyData['status']!='停牌']
        tradedays=list(dailyData['date'])
        for date in tradedays:
            path=os.path.join(self.filePath,code.replace('.','_'))
            file=os.path.join(path,date+'.h5')
            HDF5Utility.pathCreate(path)
            exists=os.path.isfile(file)
            if exists==False:
                logger.info(f'get tickshot data of {code} in {date} from source!')
                mydata=self.__getResampleTickShotDataFromSqlServer(code,date)
                store = pd.HDFStore(localFileStr,'a',complib='blosc:zstd',append=True,complevel=9)
                store.append('data',mydata,append=False,format="table",data_columns=mydata.columns)
                store.close()
            else:
                logger.info(f'Tickshot data of {code} in {date} is exists!')
                pass
        pass
    #----------------------------------------------------------------------
    def __getResampleTickShotDataFromSqlServer(self,code,date):
        mydata=self.getTickShotDataFromSqlServer(code,date)
        mydata=mydata.resample('3s',label='right',closed='right').last()
        mydata=mydata.fillna(method='ffill')
        mydata['volumeIncrease']=mydata['volume']-mydata['volume'].shift(1)
        mydata['amountIncrease']=mydata['amount']-mydata['amount'].shift(1)
        mydata=mydata[((mydata.index.time>=datetime.time(9,30)) & (mydata.index.time<=datetime.time(11,30))) | ((mydata.index.time>=datetime.time(13,00)) & (mydata.index.time<=datetime.time(15,00)))]
        return mydata
        pass
    #----------------------------------------------------------------------
    def __getTickShotDataFromSqlServer(self,code,date):
        date=str(date)
        month=date[0:6]
        database='WindFullMarket'+month
        table='MarketData_'+code.replace('.','_')
        connect=pymssql.connect( self.address,self.user,self.password,database,charset='utf8')
        cursor = connect.cursor()
        sql="select [stkcd],[tdate],left(rtrim(ttime),6)+'000' as [ttime],[cp],[S1],[S2],[S3],[S4],[S5],[B1],[B2],[B3],[B4],[B5],[SV1],[SV2],[SV3],[SV4],[SV5],[BV1],[BV2],[BV3],[BV4],[BV5],[ts],[tt] FROM [{0}].[dbo].[{1}] where [tdate]={2} and ((left(rtrim(ttime),6)>=92500 and left(rtrim(ttime),6)<=113000) or (left(rtrim(ttime),6)>=130000 and left(rtrim(ttime),6)<=150000)) order by ttime".format(database,table,date)
        cursor.execute(sql)
        mydata=cursor.fetchall()
        mydata = pd.DataFrame(mydata,columns=['code' ,'date','time' ,'lastPrice','S1','S2','S3','S4','S5','B1','B2','B3','B4','B5','SV1','SV2','SV3','SV4','SV5','BV1','BV2','BV3','BV4','BV5','volume' ,'amount'])
        mydata[['lastPrice','S1','S2','S3','S4','S5','B1','B2','B3','B4','B5','SV1','SV2','SV3','SV4','SV5','BV1','BV2','BV3','BV4','BV5','volume' ,'amount']] = mydata[['lastPrice','S1','S2','S3','S4','S5','B1','B2','B3','B4','B5','SV1','SV2','SV3','SV4','SV5','BV1','BV2','BV3','BV4','BV5','volume' ,'amount']].astype('float')
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
            self.recordResampleTickShotDataFromSqlServer(code,str(startDate),str(endDate))
           
    #----------------------------------------------------------------------
    def parallelizationGetDataByDate(self,stockCodes,startDate,endDate):
        mydata=JobLibUtility.useJobLibToGetData(self.getLotsDataByDate,stockCodes,MYGROUPS,startDate,endDate)
        return mydata
        pass
    #----------------------------------------------------------------------
    def parallelizationUpdateDataByDate(self,stockCodes,startDate,endDate):
        JobLibUtility.useJobLibToUpdateData(self.updateLotsDataByDate,stockCodes,MYGROUPS,startDate,endDate)
        pass