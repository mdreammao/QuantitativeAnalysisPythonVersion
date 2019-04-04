import datetime
import h5py
import cx_Oracle as oracle
from Config.myConfig import *
import pandas as pd
from dateutil.relativedelta import relativedelta

########################################################################
class TradedayDataProcess(object):
    """从RDF/本地文件中读取数据"""
    nowStr=datetime.datetime.now().strftime('%Y-%m-%d')
    localFileStr=LocalFileAddress+"\\tradedays.h5"
    allTradedays=None
    #----------------------------------------------------------------------
    def __init__(self):
        pass
    #----------------------------------------------------------------------
    @classmethod 
    def getTradedays(self,startDate,endDate):
        if not TradedayDataProcess.allTradedays:
            TradedayDataProcess.allTradedays=TradedayDataProcess.__getTradedaysFromLocalFile(startDate,endDate)
        else:
            pass
        startDate = datetime.datetime.strptime(str(startDate), "%Y%m%d")
        endDate = datetime.datetime.strptime(str(endDate), "%Y%m%d")
        mydata=TradedayDataProcess.allTradedays.loc[(TradedayDataProcess.allTradedays['date']>=startDate) &(TradedayDataProcess.allTradedays['date']<=endDate),'date']
        return mydata
    #----------------------------------------------------------------------
    @classmethod 
    def __getTradedaysFromLocalFile(self,startDate,endData):
        try:
            f=h5py.File(TradedayDataProcess.localFileStr,'r')
            myKeys=list(f.keys())
            f.close()
            if myKeys==[]:
                mydata=TradedayDataProcess.__getAllTradedaysFromRDF()
            else:
                lastStoreDate=datetime.datetime.strptime(max(myKeys), "%Y-%m-%d")
                print(lastStoreDate)
                if ((datetime.datetime.now() - relativedelta(months=+6))>lastStoreDate):#如果六个月没有更新，重新抓取数据
                    mydata=TradedayDataProcess.__getAllTradedaysFromRDF()
        except:
            mydata=TradedayDataProcess.__getAllTradedaysFromRDF()
        finally:
            f=h5py.File(TradedayDataProcess.localFileStr,'r')
            myKeys=list(f.keys())
            f.close()
            mydata=pd.read_hdf(TradedayDataProcess.localFileStr,key=max(myKeys))
            print(mydata)
            pass
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
        mydata['date']=pd.to_datetime(mydata['date'],format='%Y-%m-%d')
        mydata.to_hdf(TradedayDataProcess.localFileStr,key=TradedayDataProcess.nowStr,mode='a',format='table')
        #f=h5py.File(TradedayDataProcess.localFileStr,'w')
        #f.create_dataset(TradedayDataProcess.nowStr,data=mydata)
        #f.close()
        return mydata



########################################################################
