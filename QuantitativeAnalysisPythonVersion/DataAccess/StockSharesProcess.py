import datetime
import h5py
import cx_Oracle as oracle
from Config.myConfig import *
import pandas as pd
from dateutil.relativedelta import relativedelta
from DataAccess.TradedayDataProcess import *
import os
import time

########################################################################
class StockSharesProcess(object):
    """从RDF/本地文件中读取数据,获取股票股本信息和IPO信息"""
    nowStr=datetime.datetime.now().strftime('%Y%m%d')
    localFileStr=LocalFileAddress+"\\stockShares.h5"
    localFileIPOStr=LocalFileAddress+"\\stockIPODate.h5"
    localFileStockListStr=LocalFileAddress+"\\stockList.h5"
    allStockShares=pd.DataFrame()
    allStockIPOInfo=pd.DataFrame()
    allStockList=pd.DataFrame()
    #----------------------------------------------------------------------
    def __init__(self):
        pass
    #----------------------------------------------------------------------
    @classmethod 
    def getStockShares(self,code,startDate,endDate):
        code=str(code).upper()
        startDate=str(startDate)
        endDate=str(endDate)
        if len(StockSharesProcess.allStockShares)==0:
            StockSharesProcess.allStockShares=StockSharesProcess.__getStockSharesFromLocalFile()
        else:
            pass
        mydata=StockSharesProcess.allStockShares
        
        #获取日期数据
        dateList=TradedayDataProcess.getTradedays(startDate,endDate)
        #pd.to_datetime(mydata['date'],format='%Y%m%d')
        mydata=mydata[mydata['code']==code]
        select=mydata[((mydata['changeDate']<=endDate)& (mydata['changeDate'].shift(-1)>startDate) )]
        if (len(select)==0):
            mydata=mydata.iloc[-1:]
        else:
            mydata=select
        sharesData=pd.DataFrame(dateList)
        #sharesData['freeShares']=0
        for row in mydata.itertuples():
            changeStr=getattr(row, 'changeDate')
            freeShares=getattr(row,'freeShares')
            sharesData.loc[sharesData['date']>changeStr,'freeShares']=freeShares
        return sharesData
    #----------------------------------------------------------------------
    @classmethod 
    def __getStockSharesFromLocalFile(self):
        exists=os.path.isfile(StockSharesProcess.localFileStr)
        if exists==True:
            f=h5py.File(StockSharesProcess.localFileStr,'r')
            myKeys=list(f.keys())
            f.close()
            lastStoreDate=datetime.datetime.strptime(max(myKeys), "%Y%m%d")
            if (myKeys==[] or (datetime.datetime.now() - relativedelta(days=+10))>lastStoreDate):#如果六个月没有更新，重新抓取数据
                mydata=StockSharesProcess.__getAllFStockSharesFromRDF()
            else:
                store = pd.HDFStore(StockSharesProcess.localFileStr,'r')
                mydata=store.select(max(myKeys))
                store.close()
        else:
            mydata=StockSharesProcess.__getAllFStockSharesFromRDF()
        return mydata
    #----------------------------------------------------------------------
    @classmethod 
    def __getAllFStockSharesFromRDF(self):
        oracleConnectString=OracleServer['default']
        myConnection=oracle.connect(oracleConnectString)
        myCursor=myConnection.cursor()
        oracleStr="select S_INFO_WINDCODE as code,CHANGE_DT1 as changeDate,S_SHARE_FREESHARES as freeShares from wind_filesync.AShareFreeFloat order by S_INFO_WINDCODE,changeDate"
        myCursor.execute(oracleStr)
        mydata=pd.DataFrame(myCursor.fetchall(),columns=['code','changeDate','freeShares'])
        store = pd.HDFStore(StockSharesProcess.localFileStr,'a')
        store.append(StockSharesProcess.nowStr,mydata,append=False,format="table",data_columns=['code','changeDate','freeShares'])
        store.close()
        return mydata

    #----------------------------------------------------------------------
    @classmethod 
    def getStockIPOInfoByCode(self,code):
        code=str(code).upper()
        if len(StockSharesProcess.allStockIPOInfo)==0:
            StockSharesProcess.allStockIPOInfo=StockSharesProcess.__getStockIPOInfoFromLocalFile()
        else:
            pass
        mydata=StockSharesProcess.allStockIPOInfo
        mydata=mydata[mydata['code']==code]
        return mydata
    #----------------------------------------------------------------------
    @classmethod 
    def getStockListByDate(self,startDate,endDate):
        startDate=str(startDate)
        endDate=str(endDate)
        if len(StockSharesProcess.allStockList)==0:
            StockSharesProcess.allStockList=StockSharesProcess.__getStockListFromLocalFile(startDate,endDate)
        else:
            pass
        mydata=StockSharesProcess.allStockList
        mydata=mydata[(mydata['date']<=endDate) & (mydata['date']>=startDate)]
        return mydata
        pass


    #----------------------------------------------------------------------
    @classmethod 
    def __getStockIPOInfoFromLocalFile(self):
        exists=os.path.isfile(StockSharesProcess.localFileIPOStr)
        if exists==True:
            f=h5py.File(StockSharesProcess.localFileIPOStr,'r')
            myKeys=list(f.keys())
            f.close()
            lastStoreDate=datetime.datetime.strptime(max(myKeys), "%Y%m%d")
            if (myKeys==[] or (datetime.datetime.now() - relativedelta(days=+10))>lastStoreDate):#如果六个月没有更新，重新抓取数据
                mydata=StockSharesProcess.__getAllStockIPOInfoFromRDF()
            else:
                store = pd.HDFStore(StockSharesProcess.localFileIPOStr,'r')
                mydata=store.select(max(myKeys))
                store.close()
        else:
            mydata=StockSharesProcess.__getAllStockIPOInfoFromRDF()
        mydata.fillna('20991231',inplace=True)
        return mydata
    
    #----------------------------------------------------------------------
    @classmethod 
    def __getStockListFromLocalFile(self,startDate,endDate):
        allTradedays=TradedayDataProcess.getAllTradedays()
        today=time.strftime('%Y%m%d',time.localtime(time.time()))
        firstDate='20070101'
        lastDate=today
        exists=os.path.isfile(StockSharesProcess.localFileStockListStr)
        if exists==True:
            f=h5py.File(StockSharesProcess.localFileStockListStr,'r')
            myKeys=list(f.keys())
            f.close()
            if myKeys==[]:
                exists=False
        if exists==False:
            mydata=StockSharesProcess.__getStockListByDate(firstDate,lastDate)
            store = pd.HDFStore(StockSharesProcess.localFileStockListStr,'a')
            store.append('all',mydata,append=False,format="table")
            store.close()
            pass
        else:
            store = pd.HDFStore(StockSharesProcess.localFileStockListStr,'a')
            mydata=store.get('all')
            lastExitsDate=mydata['date'].max()
            if lastExistsDate<endDate:
                appendData=StockSharesProcess.__getStockListByDate(lastExistsDate,endDate)
                mydata.append(appendData,inplace=True)
                mydata.drop_duplicates(keep='first',inplace=True)
                store.append('all',mydata,append=False,format="table")
                pass
            store.close()
            pass
        return mydata
    #----------------------------------------------------------------------
    @classmethod 
    def __getStockListByDate(self,startDate,endDate):
        startDate=str(startDate)
        endDate=str(endDate)
        if len(StockSharesProcess.allStockIPOInfo)==0:
            StockSharesProcess.allStockIPOInfo=StockSharesProcess.__getStockIPOInfoFromLocalFile()
        else:
            pass
        mydata=StockSharesProcess.allStockIPOInfo
        mydata=mydata[(mydata['listDate']<=endDate) & (mydata['delistDate']>=startDate)]
        dataAll=pd.DataFrame()
        for row in mydata.itertuples():
            code=str(getattr(row, 'code'))
            name=str(getattr(row, 'name'))
            listDate=str(getattr(row, 'listDate'))
            delistDate=str(getattr(row,'delistDate'))
            #获取日期数据
            firstDate=max(listDate,startDate)
            lastDate=min(endDate,delistDate)
            dateList=TradedayDataProcess.getTradedays(firstDate,lastDate)
            stockData=pd.DataFrame(dateList,columns=['date'])
            stockData['code']=code
            stockData['name']=name
            dataAll=dataAll.append(stockData)
        dataAll=dataAll[(dataAll['date']>=startDate) & (dataAll['date']<=endDate)]
        return dataAll
        pass

    #----------------------------------------------------------------------
    @classmethod 
    def __getAllStockIPOInfoFromRDF(self):
        oracleConnectString=OracleServer['default']
        myConnection=oracle.connect(oracleConnectString)
        myCursor=myConnection.cursor()
        oracleStr="select s_info_windcode,S_INFO_NAME,S_INFO_EXCHMARKET,S_INFO_LISTBOARD,S_INFO_LISTDATE,S_INFO_DELISTDATE from wind_filesync.AShareDescription order by S_INFO_LISTDATE"
        myCursor.execute(oracleStr)
        mydata=pd.DataFrame(myCursor.fetchall(),columns=['code','name','exchange','board','listDate','delistDate'])
        store = pd.HDFStore(StockSharesProcess.localFileIPOStr,'a')
        store.append(StockSharesProcess.nowStr,mydata,append=False,format="table",data_columns=['code','name','exchange','board','listDate','delistDate'])
        store.close()
        mydata.fillna('20991231',inplace=True)
        return mydata
        pass



########################################################################
