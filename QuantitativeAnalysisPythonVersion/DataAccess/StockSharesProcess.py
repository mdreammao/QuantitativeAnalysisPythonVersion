import datetime
import h5py
import cx_Oracle as oracle
from Config.myConfig import *
import pandas as pd
from dateutil.relativedelta import relativedelta
from DataAccess.TradedayDataProcess import *
import os

########################################################################
class StockSharesProcess(object):
    """从RDF/本地文件中读取数据"""
    nowStr=datetime.datetime.now().strftime('%Y%m%d')
    localFileStr=LocalFileAddress+"\\stockShares.h5"
    allStockShares=None
    #----------------------------------------------------------------------
    def __init__(self):
        pass
    #----------------------------------------------------------------------
    @classmethod 
    def getStockShares(self,code,startDate,endDate):
        code=str(code).upper()
        startDate=str(startDate)
        endDate=str(endDate)
        if not StockSharesProcess.allStockShares:
            StockSharesProcess.allStockShares=StockSharesProcess.__getStockSharesFromLocalFile()
        else:
            pass
        mydata=StockSharesProcess.allStockShares
        
        #获取日期数据
        dateList=TradedayDataProcess.getTradedays(startDate,endDate)
        #pd.to_datetime(mydata['date'],format='%Y%m%d')
        mydata=mydata[((mydata['changeDate']<=endDate)& (mydata['changeDate'].shift(-1)>startDate) ) & (mydata['code']==code) ]
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



########################################################################
