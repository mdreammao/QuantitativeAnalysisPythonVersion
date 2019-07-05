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
from Utility.InfluxdbUtility import InfluxdbUtility
import pymssql
import influxdb
import dateutil.parser as dtparser

########################################################################
class TickDataProcess(object):
    """从170数据库获取TICK数据"""
    #----------------------------------------------------------------------
    def __init__(self,SqlSource=SqlServer['server170'],InfluxdbSource=InfluxdbServer):
        """Constructor"""
        strArry=SqlSource.split(';')
        self.address=strArry[0].split('=')[1]
        self.user=strArry[1].split('=')[1]
        self.password=strArry[2].split('=')[1]
        self.filePath=os.path.join(LocalFileAddress,'TickShots')
        self.influxdb=self.__connectInfluxdb(InfluxdbSource)
    #----------------------------------------------------------------------
    def __connectInfluxdb(self,InfluxdbSource):
        host=InfluxdbSource['host']
        port=InfluxdbSource['port']
        username=InfluxdbSource['username']
        password=InfluxdbSource['password']
        database=InfluxdbSource['database']
        client = influxdb.DataFrameClient(host=host, port=port, username=username, password=password, database=database)
        return client
        pass

    #----------------------------------------------------------------------
    def getDataByDateFromLocalFile(self,code,date):
        date=str(date)
        path=os.path.join(self.filePath,code.replace('.','_'))
        file=os.path.join(path,date+'.h5')
        HDF5Utility.pathCreate(path)
        exists=os.path.isfile(file)
        if exists==False:
            #logger.warning(f'There is no tickshot data of {code} in {date} from local file!')
            mydata=pd.DataFrame()
        else:
            #logger.info(f'get tickshot data of {code} in {date} from local file!')
            mydata=pd.DataFrame()
            try:
                with pd.HDFStore(file,'r',complib='blosc:zstd',append=True,complevel=9) as store:
                    #store = pd.HDFStore(file,'r',complib='blosc:zstd',append=True,complevel=9)
                    mydata=store['data']
                    #store.close()
            except:
                pass
            pass
        return mydata
     #----------------------------------------------------------------------
    def recordResampleTickShotDataToInfluxdbFromSqlServer(self,code,startDate,endDate):
        #获取股票日线数据，筛选出非停牌的日期
        daily=KLineDataProcess('daily')
        dailyData=daily.getDataByDate(code,startDate,endDate)
        if dailyData.empty==True:
            logger.warning(f'There no daily data of {code} from {startDate} to {endDate}!')
            return 
        dailyData=dailyData[dailyData['status']!='停牌']
        tradedays=list(dailyData['date'])
        for date in tradedays:
            logger.info(f'get tickshot data of {code} in {date} from source!')
            mydata=self.__getResampleTickShotDataFromSqlServer(code,date)
            if mydata.shape[0]>0:
                InfluxdbUtility.saveDataFrameDataToInfluxdb(mydata,INFLUXDBTICKTICKDATABASE,code,{})
        pass
    #----------------------------------------------------------------------
    def recordResampleTickShotDataFromSqlServer(self,code,startDate,endDate):
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
                logger.info(f'get tickshot data of {code} in {date} from source!')
                mydata=self.__getResampleTickShotDataFromSqlServer(code,date)
                if mydata.empty==False:
                    with pd.HDFStore(file,'a',complib='blosc:zstd',append=True,complevel=9) as store:
                        store.append('data',mydata,append=False,format="table",data_columns=mydata.columns)
                else:
                    logger.warning(f'there is no data of {code} in {date} from source!')
            else:
                logger.info(f'Tickshot data of {code} in {date} is exists!')
                pass
        pass
    #----------------------------------------------------------------------
    def __getResampleTickShotDataFromSqlServer(self,code,date):
        try:
            mydata=self.__getTickShotDataFromSqlServer(code,date)
        except:
            logger.error(f'There is no tick data of {code} in {date} from source!!')
            return pd.DataFrame()
            pass
        #计算mid价格
        select=(mydata['BV1']>0) & (mydata['SV1']>0)
        mydata.loc[select,'midPrice']=((mydata['B1']+mydata['S1'])/2)[select]
        select=(mydata['BV1']>0) & (mydata['SV1']==0)
        mydata.loc[select,'midPrice']=mydata['B1'][select]
        select=(mydata['BV1']==0) & (mydata['SV1']>0)
        mydata.loc[select,'midPrice']=mydata['S1'][select]
        #按3S重采样
        mydata['realData']=1
        mydata=mydata.resample('3s',label='right',closed='right').last()
        mycolumns=list(mydata.columns)
        mycolumns.remove('realData')
        mydata[mycolumns]=mydata[mycolumns].fillna(method='ffill')
        select=mydata['realData'].isna()
        mydata.loc[select,'realData']=0
        #成交量增量
        mydata['volumeIncrease']=mydata['volume']-mydata['volume'].shift(1)
        mydata['amountIncrease']=mydata['amount']-mydata['amount'].shift(1)
        #仅保留连续竞价时间
        mydata=mydata[((mydata.index.time>=datetime.time(9,30)) & (mydata.index.time<=datetime.time(11,30))) | ((mydata.index.time>=datetime.time(13,00)) & (mydata.index.time<=datetime.time(15,00)))]
        return mydata
        pass
    #----------------------------------------------------------------------
    def __getTickShotDataFromSqlServer(self,code,date):
        date=str(date)
        month=date[0:6]
        database='WindFullMarket'+month
        table='MarketData_'+code.replace('.','_')
        for i in range(35):
            try:
                connect=pymssql.connect( self.address,self.user,self.password,database,charset='utf8')
                break
            except pymssql.DatabaseError as err:
                time.sleep(60)
        cursor = connect.cursor()
        sql="select [stkcd],rtrim([tdate]),left(rtrim(ttime),6)+'000' as [ttime],[cp],[S1],[S2],[S3],[S4],[S5],[S6],[S7],[S8],[S9],[S10],[B1],[B2],[B3],[B4],[B5],[B6],[B7],[B8],[B9],[B10],[SV1],[SV2],[SV3],[SV4],[SV5],[SV6],[SV7],[SV8],[SV9],[SV10],[BV1],[BV2],[BV3],[BV4],[BV5],[BV6],[BV7],[BV8],[BV9],[BV10],[ts],[tt],[HighLimit],[LowLimit],[OPNPRC],[PRECLOSE],[transactions_count],[weightedAvgBidPRC],[weightedAvgAskPRC],[total_bid_size],[total_ask_size],[LocalRecTime],[TradeStatus] FROM [{0}].[dbo].[{1}] where [tdate]={2} and ((left(rtrim(ttime),6)>=92500 and left(rtrim(ttime),6)<=113000) or (left(rtrim(ttime),6)>=130000 and left(rtrim(ttime),6)<=150000)) order by ttime".format(database,table,date)
        cursor.execute(sql)
        mydata=cursor.fetchall()
        mydata = pd.DataFrame(mydata,columns=['code' ,'date','time' ,'lastPrice','S1','S2','S3','S4','S5','S6','S7','S8','S9','S10','B1','B2','B3','B4','B5','B6','B7','B8','B9','B10','SV1','SV2','SV3','SV4','SV5','SV6','SV7','SV8','SV9','SV10','BV1','BV2','BV3','BV4','BV5','BV6','BV7','BV8','BV9','BV10','volume' ,'amount','highLimit','lowLimit','dailyOpen','dailyPreClose','transactions_count','weightedAvgBid','weightedAvgAsk','total_bid_size','total_ask_size','localRecordTime','tradeStatus'])
        mydata[['lastPrice','S1','S2','S3','S4','S5','S6','S7','S8','S9','S10','B1','B2','B3','B4','B5','B6','B7','B8','B9','B10','SV1','SV2','SV3','SV4','SV5','SV6','SV7','SV8','SV9','SV10','BV1','BV2','BV3','BV4','BV5','BV6','BV7','BV8','BV9','BV10','volume' ,'amount','highLimit','lowLimit','dailyOpen','dailyPreClose','transactions_count','weightedAvgBid','weightedAvgAsk','total_bid_size','total_ask_size','tradeStatus']] = mydata[['lastPrice','S1','S2','S3','S4','S5','S6','S7','S8','S9','S10','B1','B2','B3','B4','B5','B6','B7','B8','B9','B10','SV1','SV2','SV3','SV4','SV5','SV6','SV7','SV8','SV9','SV10','BV1','BV2','BV3','BV4','BV5','BV6','BV7','BV8','BV9','BV10','volume' ,'amount','highLimit','lowLimit','dailyOpen','dailyPreClose','transactions_count','weightedAvgBid','weightedAvgAsk','total_bid_size','total_ask_size','tradeStatus']].astype('float')
        mydata['mytime']=pd.to_datetime(mydata['date']+mydata['time'],format='%Y%m%d%H%M%S%f')
        mydata.set_index('mytime',inplace=True,drop=True)
        return mydata    

        pass
    #----------------------------------------------------------------------
    def getTickShotDataFromInfluxdbServer(self,code,date,database=INFLUXDBTICKTICKDATABASE):
        measurement=code
        begin=dtparser.parse(str(date))+datetime.timedelta(hours=0)
        end=dtparser.parse(str(date))+datetime.timedelta(hours=24)
        query = f""" select * from "{database}"."autogen"."{measurement}" where time >= {int(begin.timestamp() * 1000 * 1000 * 1000)} and time < {int(end.timestamp() * 1000 * 1000* 1000)} """
        result=self.influxdb.query(query)
        data=pd.DataFrame(data)
        try:
            data=result[measurement]
        except:
            pass
        return data
        pass
    #----------------------------------------------------------------------
    '''
    def getResampleTickShotDataFromInfluxdbServer(self,code,date):
        measurement=code+'_snapshot'
        begin=dtparser.parse(str(date))
        end=dtparser.parse(str(date))+datetime.timedelta(hours=23)
        query = f""" select * from "MarketSnapshotDB"."autogen"."{measurement}" where time >= {int(begin.timestamp() * 1000 * 1000 * 1000)} and time < {int(end.timestamp() * 1000 * 1000* 1000)} """
        result=self.influxdb.query(query)
        data=result[measurement]
        data=pd.DataFrame(data)
        renameDict={'HighLimit':'highLimit', 'LowLimit':'lowLimit', 'OPNPRC':'dailyOpen', 'PRECLOSE':'dailyPreClose', 'TradeStatus':'tradeStatus', 'cp':'lastPrice', 'stkcd':'code', 'tdate':'date','time':'influxdbTime', 'ts':'volume', 'tt':'amount',
       'ttime':'time', 'turnover':'amountIncrease', 'volume':'volumeIncrease', 'weightedAvgAskPRC':'weightedAvgAsk',
       'weightedAvgBidPRC':'weightedAvgBid'}
        mydata=data.rename(columns=renameDict)
        mydata=mydata[['code' ,'date','time' ,'lastPrice','S1','S2','S3','S4','S5','S6','S7','S8','S9','S10','B1','B2','B3','B4','B5','B6','B7','B8','B9','B10','SV1','SV2','SV3','SV4','SV5','SV6','SV7','SV8','SV9','SV10','BV1','BV2','BV3','BV4','BV5','BV6','BV7','BV8','BV9','BV10','volume','amount','highLimit','lowLimit','dailyOpen','dailyPreClose','transactions_count','weightedAvgBid','weightedAvgAsk','total_bid_size','total_ask_size','tradeStatus']]
        mydata=mydata[((mydata['time']>='092500000') & (mydata['time']<='113000000'))| ((mydata['time']>='130000000')&(mydata['time']<='150000000'))]
        mydata['mytime']=pd.to_datetime(mydata['date']+mydata['time'],format='%Y%m%d%H%M%S%f')
        mydata.set_index('mytime',inplace=True,drop=True)
        #计算mid价格
        select=(mydata['BV1']>0) & (mydata['SV1']>0)
        mydata.loc[select,'midPrice']=((mydata['B1']+mydata['S1'])/2)[select]
        select=(mydata['BV1']>0) & (mydata['SV1']==0)
        mydata.loc[select,'midPrice']=mydata['B1'][select]
        select=(mydata['BV1']==0) & (mydata['SV1']>0)
        mydata.loc[select,'midPrice']=mydata['S1'][select]
        #按3S重采样
        mydata['realData']=1
        mydata=mydata.resample('3s',label='right',closed='right').last()
        mycolumns=list(mydata.columns)
        mycolumns.remove('realData')
        mydata[mycolumns]=mydata[mycolumns].fillna(method='ffill')
        select=mydata['realData'].isna()
        mydata.loc[select,'realData']=0
        #成交量增量
        mydata['volumeIncrease']=mydata['volume']-mydata['volume'].shift(1)
        mydata['amountIncrease']=mydata['amount']-mydata['amount'].shift(1)
        #仅保留连续竞价时间
        mydata=mydata[((mydata.index.time>=datetime.time(9,30)) & (mydata.index.time<=datetime.time(11,30))) | ((mydata.index.time>=datetime.time(13,00)) & (mydata.index.time<=datetime.time(15,00)))]
        return mydata
        pass
    '''
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

    #----------------------------------------------------------------------
    #输入code=600000.SH，startdate=yyyyMMdd，endDate=yyyyMMdd
    def updateLotsDataToInfluxdbByDate(self,StockCodes,startDate,endDate):
        mydata=pd.DataFrame()
        for i in range(len(StockCodes)):
            code=StockCodes[i]
            self.recordResampleTickShotDataToInfluxdbFromSqlServer(code,str(startDate),str(endDate))

    #----------------------------------------------------------------------
    def parallelizationUpdateDataToInfluxdbByDate(self,stockCodes,startDate,endDate):
        JobLibUtility.useJobLibToUpdateData(self.updateLotsDataToInfluxdbByDate,stockCodes,MYGROUPS,startDate,endDate)
        pass
