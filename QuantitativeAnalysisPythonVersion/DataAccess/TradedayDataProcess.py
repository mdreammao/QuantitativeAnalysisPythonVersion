import datetime
import h5py
import cx_Oracle as oracle
from Config.myConfig import *
import pandas as pd
from dateutil.relativedelta import relativedelta
import os

########################################################################
class TradedayDataProcess(object):
    """从RDF/本地文件中读取数据"""
    nowStr=datetime.datetime.now().strftime('%Y%m%d')
    localFileStr=LocalFileAddress+"\\tradedays.h5"
    allTradedays=pd.DataFrame()
    #----------------------------------------------------------------------
    def __init__(self):
        pass
    #----------------------------------------------------------------------
    @classmethod 
    def getTradedays(self,startDate,endDate):
        startDate=str(startDate)
        endDate=str(endDate)
        if len(TradedayDataProcess.allTradedays)==0:
            TradedayDataProcess.allTradedays=TradedayDataProcess.__getTradedaysFromLocalFile(startDate,endDate)
        else:
            pass
        #startDate = datetime.datetime.strptime(startDate, "%Y%m%d")
        #endDate = datetime.datetime.strptime(endDate, "%Y%m%d")
        mydata=TradedayDataProcess.allTradedays.loc[(TradedayDataProcess.allTradedays['date']>=startDate) &(TradedayDataProcess.allTradedays['date']<=endDate),'date']
        return mydata
    #----------------------------------------------------------------------
    @classmethod 
    def __getTradedaysFromLocalFile(self,startDate,endData):
        exists=os.path.isfile(TradedayDataProcess.localFileStr)
        if exists==True:
            f=h5py.File(TradedayDataProcess.localFileStr,'r')
            myKeys=list(f.keys())
            f.close()
            lastStoreDate=datetime.datetime.strptime(max(myKeys), "%Y%m%d")
            if (myKeys==[] or (datetime.datetime.now() - relativedelta(months=+6))>lastStoreDate):#如果六个月没有更新，重新抓取数据
                mydata=TradedayDataProcess.__getAllTradedaysFromRDF()
            else:
                store = pd.HDFStore(TradedayDataProcess.localFileStr,'r')
                mydata=store.select(max(myKeys))
                store.close()
        else:
            mydata=TradedayDataProcess.__getAllTradedaysFromRDF()
        return mydata
    #----------------------------------------------------------------------
    @classmethod 
    def __getAllTradedaysFromRDF(self):
        oracleConnectString=OracleServer['default']
        myConnection=oracle.connect(oracleConnectString)
        myCursor=myConnection.cursor()
        myCursor.execute('''
        select trade_days from wind_filesync.asharecalendar where s_info_exchmarket='SSE' order by trade_days
        ''')
        mydata=pd.DataFrame(myCursor.fetchall(),columns=['date'])
        #mydata['date']=mydata['date'].astype(str)
       # mydata['date']=pd.to_datetime(mydata['date'],format='%Y%m%d')
        store = pd.HDFStore(TradedayDataProcess.localFileStr,'a')
        store.append(TradedayDataProcess.nowStr,mydata,append=False,format="table",data_columns=['date'])
        store.close()
        return mydata



########################################################################
