import pymssql
import cx_Oracle as oracle
import pandas as pd
from Config.myConstant import *
from Config.myConfig import *
from DataAccess.TradedayDataProcess import *
import datetime
import h5py
import os


########################################################################
class IndexComponentDataProcess(object):
    """从SqlServer/RDF/本地文件中获取数据"""
    #----------------------------------------------------------------------
    def __init__(self,update=False,OracleSource=OracleServer['default']):
        """Constructor"""
        self.update=update
        self.key='indexComponent'
        self.indexMembers='indexMembers'
        self.oracleConnectStr=OracleSource
    #----------------------------------------------------------------------
    #输入code=000300.SH，startdate=yyyyMMdd，endDate=yyyyMMdd
    def __getDataByDateFromSource(self,code,startDate=EMPTY_STRING,endDate=EMPTY_STRING):
        code=str(code).upper()
        startDate=str(startDate)
        endDate=str(endDate)
        return self.__getDailyDataByDateFromOracleServer(code,startDate,endDate)
    #----------------------------------------------------------------------
    #输入code=000300.SH，startdate=yyyyMMdd，endDate=yyyyMMdd
    def __getDailyDataByDateFromOracleServer(self,code,startDate=EMPTY_STRING,endDate=EMPTY_STRING):
        if code=='000300.SH':
            oracleStr="select TRADE_DT as \"date\",S_INFO_WINDCODE as indexCode,S_CON_WINDCODE as code,	I_WEIGHT_11 as totalShares,I_WEIGHT_12 as freeSharesRatio,S_IN_INDEX as sharesInIndex,I_WEIGHT_14 as weightFactor,I_WEIGHT as weight,I_WEIGHT_15 as preClose,I_WEIGHT_16 as preCloseAdjusted,I_WEIGHT_17 as totalMarketValue,I_WEIGHT_18 as marketValueInIndex from wind_filesync.AIndexHS300Weight "
        elif code=='000905.SH':
            oracleStr="select TRADE_DT as \"date\",S_INFO_WINDCODE as indexCode,S_CON_WINDCODE as code,tot_shr as totalShares,free_shr_ratio as freeSharesRatio,shr_calculation as sharesInIndex,weightfactor as weightFactor,weight as weight,closevalue as preClose,open_adjusted as preCloseAdjusted,tot_mv as totalMarketValue,mv_calculation as marketValueInIndex from wind_filesync.AIndexCSI500Weight "
        elif code=='000016.SH':
            oracleStr="select TRADE_DT as \"date\",S_INFO_WINDCODE as indexCode,S_CON_WINDCODE as code,tot_shr as totalShares,free_shr_ratio as freeSharesRatio,shr_calculation as sharesInIndex,weightfactor as weightFactor,weight as weight,closevalue as preClose,open_adjusted as preCloseAdjusted,tot_mv as totalMarketValue,mv_calculation as marketValueInIndex from wind_filesync.AIndexSSE50Weight "
        connection = oracle.connect(self.oracleConnectStr)
        cursor = connection.cursor()
        if startDate==EMPTY_STRING:
            oracleStr=oracleStr+"order by TRADE_DT"
        elif endDate==EMPTY_STRING:
            oracleStr=oracleStr+"where TRADE_DT>={0} order by TRADE_DT".format(startDate)
        else:
            oracleStr=oracleStr+"where TRADE_DT>={0} and TRADE_DT<={1}order by TRADE_DT".format(startDate,endDate)
        cursor.execute(oracleStr)
        mydata = cursor.fetchall()
        mydata = pd.DataFrame(mydata,columns=['date','indexCode','code','totalShares','freeSharesRatio','sharesInIndex','weightFactor','weight','preClose','preCloseAdjusted','totalMarketValue','marketValueInIndex'])
        mydata[['totalShares','freeSharesRatio','sharesInIndex','weightFactor','weight','preClose','preCloseAdjusted','totalMarketValue','marketValueInIndex']] = mydata[['totalShares','freeSharesRatio','sharesInIndex','weightFactor','weight','preClose','preCloseAdjusted','totalMarketValue','marketValueInIndex']].astype('float')
        return mydata  
    #----------------------------------------------------------------------
    #输入code=000300.SH，startdate=yyyyMMdd，endDate=yyyyMMdd
    def __getDailyDataByIndexCodeFromOracleServer(self,indexCode):
        oracleStr="select S_INFO_WINDCODE as \"indexCode\",S_CON_WINDCODE as \"code\",S_CON_INDATE as \"entry\",S_CON_OUTDATE as \"remove\" from wind_filesync.AIndexMembers where  S_INFO_WINDCODE='{0}'".format(indexCode)
        connection = oracle.connect(self.oracleConnectStr)
        cursor = connection.cursor()
        cursor.execute(oracleStr)
        mydata = cursor.fetchall()
        mydata = pd.DataFrame(mydata,columns=['indexCode','code','entry','remove'])
        mydata['updateDate']=datetime.datetime.now().strftime("%Y%m%d")
        return mydata  
    #----------------------------------------------------------------------
    #输入code=000300.SH，startdate=yyyyMMdd，endDate=yyyyMMdd
    def __getDataByDateFromLocalFile(self,code,startDate,endDate):
        code=str(code).upper();
        localFileStr=LocalFileAddress+"\\{0}\\{1}.h5".format('index',code.replace('.','_'))
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
                mydata=store.select(self.key)
                store.close()
                if endDate==EMPTY_STRING or mydata['date'].max()<endDate:
                    latestDate=datetime.datetime.strptime(mydata['date'].max(),"%Y%m%d")
                    updateDate=(latestDate+datetime.timedelta(days=1)).strftime("%Y%m%d")
                    updateData=self.__getDailyDataByDateFromOracleServer(code,updateDate)
                    self.__saveDataToLocalFile(localFileStr,updateData)
        store = pd.HDFStore(localFileStr,'a')
        mydata=store.select(self.key,where=['date>="%s" and date<="%s"'%(startDate,endDate)])
        store.close()
        return mydata
    #----------------------------------------------------------------------
    #输入code=000300.SH，startdate=yyyyMMdd，endDate=yyyyMMdd
    def __getDataByIndexCodeFromLocalFile(self,indexCode):
        code=str(indexCode).upper();
        localFileStr=LocalFileAddress+"\\{0}\\{1}.h5".format('index',self.indexMembers)
        exists=os.path.isfile(localFileStr)
        if exists==True:
            f=h5py.File(localFileStr,'r')
            myKeys=list(f.keys())
            f.close()
            if (myKeys==[]) or (indexCode not in myKeys):
                exists=False
        if exists==False:
            mydata=self.__getDailyDataByIndexCodeFromOracleServer(indexCode)
            self.__saveDataToLocalFileByIndexCode(indexCode,localFileStr,mydata)
            return mydata
        else:
            if self.update==True:
                mydata=self.__getDailyDataByIndexCodeFromOracleServer(indexCode)
                self.__saveDataToLocalFileByIndexCode(indexCode,localFileStr,mydata)
                return mydata
        store = pd.HDFStore(localFileStr,'a')
        mydata=store.select(indexCode)
        store.close()
        latestDate=datetime.datetime.strptime(mydata['updateDate'].max(),"%Y%m%d")
        needUpdateDate=latestDate+datetime.timedelta(days=30)
        if needUpdateDate<datetime.datetime.now():
            mydata=self.__getDailyDataByIndexCodeFromOracleServer(indexCode)
            self.__saveDataToLocalFileByIndexCode(indexCode,localFileStr,mydata)
        return mydata
    #----------------------------------------------------------------------
    #输入code=000016.SH，startdate=yyyyMMdd，endDate=yyyyMMdd
    def getSSE50DataByDate(self,startDate,endDate):
        localdata=self.__getDataByDateFromLocalFile('000016.SH',str(startDate),str(endDate))
        return localdata
    #----------------------------------------------------------------------
    #输入code=000300.SH，startdate=yyyyMMdd，endDate=yyyyMMdd
    def getHS300DataByDate(self,startDate,endDate):
        localdata=self.__getDataByDateFromLocalFile('000300.SH',str(startDate),str(endDate))
        return localdata
    #----------------------------------------------------------------------
    #输入code=000905.SH，startdate=yyyyMMdd，endDate=yyyyMMdd
    def getCSI500DataByDate(self,startDate,endDate):
        localdata=self.__getDataByDateFromLocalFile('000905.SH',str(startDate),str(endDate))
        return localdata
    #----------------------------------------------------------------------
    def getStockBelongs(self,code,indexCode,startDate,endDate):
        mydata=self.__getDataByIndexCodeFromLocalFile(indexCode)
        tradedays=TradedayDataProcess.getTradedays(startDate,endDate)
        dataWithIndex=pd.DataFrame(tradedays,columns=['date'])
        #dataWithIndex.set_index(['date'],inplace=True)
        mydata=mydata[mydata['code']==code]
        mydata.fillna('20991231',inplace=True)
        dataWithIndex['exists']=0
        for row in range(len(mydata)):
            entry=mydata.iloc[row]['entry']
            remove=mydata.iloc[row]['remove']
            dataWithIndex.loc[((dataWithIndex['date']>=entry) & (dataWithIndex['date']<remove)),'exists']=1
        return dataWithIndex
    #----------------------------------------------------------------------
    #输入code=000300.SH，startdate=yyyyMMdd，endDate=yyyyMMdd
    def __saveDataToLocalFile(self,localFileStr,data):
        store = pd.HDFStore(localFileStr,'a')
        store.append(self.key,data,append=True,format="table",data_columns=['date','indexCode','code','totalShares','freeSharesRatio','sharesInIndex','weightFactor','weight','preClose','preCloseAdjusted','totalMarketValue','marketValueInIndex'])
        store.close()
    #----------------------------------------------------------------------
    #输入code=000300.SH，startdate=yyyyMMdd，endDate=yyyyMMdd
    def __saveDataToLocalFileByIndexCode(self,indexCode,localFileStr,data):
        store = pd.HDFStore(localFileStr,'a')
        store.append(indexCode,data,append=False,format="table",data_columns=['indexCode','code','entry','remove','updateDate'])
        store.close()
########################################################################
