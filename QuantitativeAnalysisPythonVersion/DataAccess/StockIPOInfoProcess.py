import datetime
import h5py
import cx_Oracle as oracle
from Config.myConfig import *
from Config.myConstant import *
import pandas as pd
from dateutil.relativedelta import relativedelta
from DataAccess.TradedayDataProcess import *
import os
import time

########################################################################
class StockIPOInfoProcess(object):
    """从RDF/本地文件中读取数据,获取股票股本信息和IPO信息"""
    nowStr=datetime.datetime.now().strftime('%Y%m%d')
    localFileIPOStr=os.path.join(LocalFileAddress,'stockIPODate.h5')
    localFileStockListStr=os.path.join(LocalFileAddress,'codeList.h5')
    allStockIPOInfo=pd.DataFrame()
    allStockList=pd.DataFrame()
    #----------------------------------------------------------------------
    @classmethod 
    def getStockIPOInfoByCode(self,code):
        code=str(code).upper()
        if len(StockIPOInfoProcess.allStockIPOInfo)==0:
            StockIPOInfoProcess.allStockIPOInfo=StockIPOInfoProcess.__getStockIPOInfoFromLocalFile()
        else:
            pass
        mydata=StockIPOInfoProcess.allStockIPOInfo
        mydata=mydata[mydata['code']==code]
        return mydata
    #----------------------------------------------------------------------
    @classmethod 
    def getStockIPOInfoByDate(self,startDate,endDate):
        startDate=str(startDate)
        endDate=str(endDate)
        if len(StockIPOInfoProcess.allStockIPOInfo)==0:
            StockIPOInfoProcess.allStockIPOInfo=StockIPOInfoProcess.__getStockIPOInfoFromLocalFile()
        else:
            pass
        mydata=StockIPOInfoProcess.allStockIPOInfo
        mydata=mydata[(mydata['listDate']<=endDate) & (mydata['delistDate']>=startDate)]
        return mydata
    #----------------------------------------------------------------------
    @classmethod 
    def updateIPOInfoFromLocalFile(self):
        mydata=StockIPOInfoProcess.__getAllStockIPOInfoFromRDF()
        if mydata.empty==True:
            logger.error(f'There is no IPO data from source!')
            return
            pass
        mydata.fillna('20991231',inplace=True)
        exists=os.path.isfile(StockIPOInfoProcess.localFileIPOStr)
        if exists==True:
            os.remove(StockIPOInfoProcess.localFileIPOStr)
        logger.info(f'Update stock IPO Info!')
        store = pd.HDFStore(StockIPOInfoProcess.localFileIPOStr,'a',complib='blosc:zstd',append=True,complevel=9)
        store.append('data',mydata,append=False,format="table",data_columns=mydata.columns)
        store.close()
        pass
    #----------------------------------------------------------------------
    @classmethod 
    def getStockListByDate(self,startDate,endDate):
        startDate=str(startDate)
        endDate=str(endDate)
        if len(StockIPOInfoProcess.allStockList)==0:
            StockIPOInfoProcess.allStockList=StockIPOInfoProcess.__getStockListFromLocalFile()
        else:
            pass
        mydata=StockIPOInfoProcess.allStockList
        mydata=mydata[(mydata['date']<=endDate) & (mydata['date']>=startDate)]
        return mydata
        pass


    #----------------------------------------------------------------------
    @classmethod 
    def __getStockIPOInfoFromLocalFile(self):
        exists=os.path.isfile(StockIPOInfoProcess.localFileIPOStr)
        if exists==True:
            store = pd.HDFStore(StockIPOInfoProcess.localFileIPOStr,'r')
            mydata=store['data']
            store.close()
            mydata.fillna('20991231',inplace=True)
        else:
            logger.error(f'There is no IPO data from local file!')
            mydata=pd.DataFrame()
        return mydata
    
    #----------------------------------------------------------------------
    @classmethod 
    def updateStockListFromLocalFile(self,startDate,endDate):
        firstDate=FIRSTDATE
        lastDate=endDate
        exists=os.path.isfile(StockIPOInfoProcess.localFileStockListStr)
        if exists==False:
            mydata=StockIPOInfoProcess.__getStockListByDate(firstDate,lastDate)
            mydate=mydata['date'].drop_duplicates()
            store = pd.HDFStore(StockIPOInfoProcess.localFileStockListStr,'a',complib='blosc:zstd',append=True,complevel=9)
            store.append('data',mydata,append=False,format="table",data_columns=mydata.columns)
            store.append('date',mydate,append=False,format="table")
            store.close()
            pass
        else:
            store = pd.HDFStore(StockIPOInfoProcess.localFileStockListStr,'a',complib='blosc:zstd',append=True,complevel=9)
            mydate=store['date']
            firstDate=TradedayDataProcess.getNextTradeday(mydate.max())
            if firstDate<=lastDate:
                mydata=StockIPOInfoProcess.__getStockListByDate(firstDate,lastDate)
                if mydata.empty==False:
                    mydate=mydata['date'].drop_duplicates()
                    store.append('data',mydata,append=True,format="table",data_columns=mydata.columns)
                    store.append('date',mydate,append=False,format="table")
            store.close()
            pass
        pass
    #----------------------------------------------------------------------
    @classmethod 
    def __getStockListFromLocalFile(self):
        exists=os.path.isfile(StockIPOInfoProcess.localFileStockListStr)
        if exists==True:
            store = pd.HDFStore(StockIPOInfoProcess.localFileStockListStr,'a')
            mydata=store['data']
            store.close()
        if exists==False:
            mydata=pd.DataFrame()
            logger.error(f'There is no stockList data from local File!')
            pass
        return mydata
    #----------------------------------------------------------------------
    @classmethod 
    def __getStockListByDate(self,startDate,endDate):
        startDate=str(startDate)
        endDate=str(endDate)
        if len(StockIPOInfoProcess.allStockIPOInfo)==0:
            StockIPOInfoProcess.allStockIPOInfo=StockIPOInfoProcess.__getStockIPOInfoFromLocalFile()
        else:
            pass
        mydata=StockIPOInfoProcess.allStockIPOInfo
        mydata=mydata[(mydata['listDate']<=endDate) & (mydata['delistDate']>startDate)]
        dataAll=pd.DataFrame()
        dateList=list(TradedayDataProcess.getTradedays(startDate,endDate))
        for date in dateList:
            logger.info(f'stockList of {date} start!')
            tmpdata=mydata[(mydata['listDate']<=date) & (mydata['delistDate']>date)]
            tmpdata=tmpdata[['code','name','exchange']]
            stockData=tmpdata.copy(deep=True)
            stockData['date']=date
            dataAll=dataAll.append(stockData)
            pass
        '''
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
        '''
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
        store = pd.HDFStore(StockIPOInfoProcess.localFileIPOStr,'a')
        store.append(StockIPOInfoProcess.nowStr,mydata,append=False,format="table",data_columns=['code','name','exchange','board','listDate','delistDate'])
        store.close()
        mydata.fillna('20991231',inplace=True)
        return mydata
        pass



########################################################################
