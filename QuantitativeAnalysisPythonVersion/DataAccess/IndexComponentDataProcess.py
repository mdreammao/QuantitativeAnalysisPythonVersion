import pymssql
import cx_Oracle as oracle
import pandas as pd
from Config.myConstant import *
from Config.myConfig import *
from DataAccess.TradedayDataProcess import *
from Utility.HDF5Utility import *
import datetime
import h5py
import os


########################################################################
class IndexComponentDataProcess(object):
    """从SqlServer/RDF/本地文件中获取数据"""
    indexMembersData=pd.DataFrame()
    indexComponentData=pd.DataFrame()
    indexList=[HS300,SSE50,CSI500]
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
    #指数股票成员信息
    def updateIndexComponentFromLocalFile(self,code,startDate,endDate):
        code=str(code).upper();
        localFilePath=os.path.join(LocalFileAddress,'index',self.key)
        HDF5Utility.pathCreate(localFilePath)
        localFileStr=os.path.join(LocalFileAddress,'index',self.key,code.replace('.','_')+'.h5')
        exists=os.path.isfile(localFileStr)
        if exists==False:
            mydata=self.__getDataByDateFromSource(code)
            self.__saveDataToLocalFile(localFileStr,mydata)
        else:
            store = pd.HDFStore(localFileStr,'r')
            date=store['date']
            store.close()
            updateDate=TradedayDataProcess.getNextTradeday(date.max())
            updateData=self.__getDailyDataByDateFromOracleServer(code,updateDate,endDate)
            if updateData.empty==False:
                self.__saveDataToLocalFile(localFileStr,updateData)
            
    #----------------------------------------------------------------------
    #输入code=000300.SH，startdate=yyyyMMdd，endDate=yyyyMMdd
    #指数股票成员信息
    def __getIndexComponentFromLocalFile(self,code,startDate,endDate):
        code=str(code).upper();
        localFilePath=os.path.join(LocalFileAddress,'index',self.key)
        HDF5Utility.pathCreate(localFilePath)
        localFileStr=os.path.join(LocalFileAddress,'index',self.key,code.replace('.','_')+'.h5')
        if self.update==True:
            self.updateIndexComponentFromLocalFile(code,startDate,endDate)
            pass
        exists=os.path.isfile(localFileStr)
        if exists==True:
            store = pd.HDFStore(localFileStr,'r')
            mydata=store.select('data',where=['date>="%s" and date<="%s"'%(startDate,endDate)])
            store.close()
        else:
            logger.waring(f'There is no component data of {code}')
            mydata=pd.DataFrame()
            pass
        return mydata
    #----------------------------------------------------------------------
    #输入code=000300.SH，startdate=yyyyMMdd，endDate=yyyyMMdd
    #股票进出指数信息
    def updateIndexEntryAndRemoveFromLocalFile(self,indexCode):
        code=str(indexCode).upper();
        #localFileStr=LocalFileAddress+"\\{0}\\{1}.h5".format('index',self.indexMembers)
        localFilePath=os.path.join(LocalFileAddress,'index',self.indexMembers)
        HDF5Utility.pathCreate(localFilePath)
        localFileStr=os.path.join(LocalFileAddress,'index',self.indexMembers,code.replace('.','_')+'.h5')
        exists=os.path.isfile(localFileStr)
        mydata=self.__getDailyDataByIndexCodeFromOracleServer(indexCode)
        self.__saveDataToLocalFileByIndexCode(indexCode,localFileStr,mydata)
        pass
    #----------------------------------------------------------------------
    #输入code=000300.SH，startdate=yyyyMMdd，endDate=yyyyMMdd
    #股票进出指数信息
    def __getIndexEntryAndRemoveFromLocalFile(self,indexCode):
        code=str(indexCode).upper();
        localFilePath=os.path.join(LocalFileAddress,'index',self.indexMembers)
        HDF5Utility.pathCreate(localFilePath)
        localFileStr=os.path.join(LocalFileAddress,'index',self.indexMembers,code.replace('.','_')+'.h5')
        if self.update==True:
            self.updateIndexEntryAndRemoveFromLocalFile(indexCode)
            pass
        exists=os.path.isfile(localFileStr)
        if exists==True:
            store = pd.HDFStore(localFileStr,'r')
            mydata=store['data']
            store.close()
            pass
        else:
            mydata=pd.DataFrame()
            logger.warning(f'There is no {indexCode} member data!!')
            pass
        return mydata
    #----------------------------------------------------------------------
    #输入code=000016.SH，startdate=yyyyMMdd，endDate=yyyyMMdd
    def getSSE50DataByDate(self,startDate,endDate):
        localdata=self.__getIndexComponentFromLocalFile('000016.SH',str(startDate),str(endDate))
        return localdata
    #----------------------------------------------------------------------
    #输入code=000300.SH，startdate=yyyyMMdd，endDate=yyyyMMdd
    def getHS300DataByDate(self,startDate,endDate):
        localdata=self.__getIndexComponentFromLocalFile('000300.SH',str(startDate),str(endDate))
        return localdata
    #----------------------------------------------------------------------
    #输入code=000905.SH，startdate=yyyyMMdd，endDate=yyyyMMdd
    def getCSI500DataByDate(self,startDate,endDate):
        localdata=self.__getIndexComponentFromLocalFile('000905.SH',str(startDate),str(endDate))
        return localdata
    #----------------------------------------------------------------------
    def getIndexMember(self,indexCode,startDate,endDate):
        indexCode=str(indexCode)
        mydata=self.__getIndexEntryAndRemoveFromLocalFile(indexCode)
    #----------------------------------------------------------------------
    #获取股票在指数中的属性
    @classmethod
    def getStockPropertyInIndex(self,code,indexCode,startDate,endDate):
        if len(IndexComponentDataProcess.indexMembersData)==0:
            IndexComponentDataProcess.indexMembersData=IndexComponentDataProcess.getIndexComponentAllFromLocalFile()
        mydata=IndexComponentDataProcess.indexMembersData
        mydata=mydata[(mydata['date']>=startDate) & (mydata['date']<=endDate) & (mydata['indexCode']==indexCode)]
        mydata=mydata[mydata['code']==code]
        tradedays=TradedayDataProcess.getTradedays(startDate,endDate)
        days=pd.DataFrame(tradedays,columns=['date'])
        mydata=pd.merge(days,mydata,how='left',left_on='date',right_on='date')
        mydata.set_index('date',drop=True,inplace=True)
        return mydata
    #----------------------------------------------------------------------
    #获取股票在指数中的属性
    @classmethod
    def getIndexComponentAllFromLocalFile(self):
        mydataAll=pd.DataFrame()
        for code in IndexComponentDataProcess.indexList:
            code=str(code).upper();
            localFilePath=os.path.join(LocalFileAddress,'index','indexComponent')
            HDF5Utility.pathCreate(localFilePath)
            localFileStr=os.path.join(LocalFileAddress,'index','indexComponent',code.replace('.','_')+'.h5')
            exists=os.path.isfile(localFileStr)
            if exists==True:
                store = pd.HDFStore(localFileStr,'r')
                mydata=store.select('data')
                store.close()
            else:
                logger.waring(f'There is no component data of {code}')
                mydata=pd.DataFrame()
                pass
            mydataAll=mydataAll.append(mydata)
            pass
        return mydataAll
        pass
    #----------------------------------------------------------------------
    #获取股票是否属于某指数
    def getStockBelongs(self,code,indexCode,startDate,endDate):
        code=str(code)
        indexCode=str(indexCode)
        mydata=self.__getIndexEntryAndRemoveFromLocalFile(indexCode)
        tradedays=TradedayDataProcess.getTradedays(startDate,endDate)
        dataWithIndex=pd.DataFrame(tradedays,columns=['date'])
        #dataWithIndex.set_index(['date'],inplace=True)
        mydata=mydata[mydata['code']==code]
        mydata.fillna('20991231',inplace=True)
        dataWithIndex['exists']=0.0
        for row in range(len(mydata)):
            entry=mydata.iloc[row]['entry']
            remove=mydata.iloc[row]['remove']
            dataWithIndex.loc[((dataWithIndex['date']>=entry) & (dataWithIndex['date']<remove)),'exists']=1.0
        dataWithIndex.set_index('date',drop=True,inplace=True)
        return dataWithIndex
    #----------------------------------------------------------------------
    #输入code=000300.SH，startdate=yyyyMMdd，endDate=yyyyMMdd
    #存储指数的成员信息
    def __saveDataToLocalFile(self,localFileStr,data):
        store = pd.HDFStore(localFileStr,'a',complib='blosc:zstd',append=True,complevel=9)
        date=data['date'].drop_duplicates()
        store.append('date',date,append=True,format="table",data_columns=['date'])
        #store.append('data',data,append=True,format="table",data_columns=['date','indexCode','code','totalShares','freeSharesRatio','sharesInIndex','weightFactor','weight','preClose','preCloseAdjusted','totalMarketValue','marketValueInIndex'])
        store.append('data',data,append=True,format="table",data_columns=data.columns)
        store.close()
    #----------------------------------------------------------------------
    #输入code=000300.SH，startdate=yyyyMMdd，endDate=yyyyMMdd
    #存储指数股票进出信息
    def __saveDataToLocalFileByIndexCode(self,indexCode,localFileStr,data):
        store = pd.HDFStore(localFileStr,'a',complib='blosc:zstd',append=True,complevel=9)
        store.append('data',data,append=False,format="table",data_columns=['indexCode','code','entry','remove','updateDate'])
        store.close()
########################################################################
