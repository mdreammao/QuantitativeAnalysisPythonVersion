import pymssql
import cx_Oracle as oracle
import pandas as pd
from Config.myConstant import *
from Config.myConfig import *
import datetime
import h5py
import os
from dateutil.relativedelta import relativedelta

########################################################################
class IndexCode(object):
    """指数信息"""
    nowStr=datetime.datetime.now().strftime('%Y%m%d')
    localFileStr=LocalFileAddress+"\\indexCode.h5"
    allIndex=None
    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        pass
    #----------------------------------------------------------------------
    #返还指数信息，'code'指数代码,'name'指数名称,'code2'指数数据库内部代码,'name2'指数数据库内部名称
    @classmethod 
    def getIndexCodeInfo(self):
        if not IndexCode.allIndex:
            IndexCode.allIndex=IndexCode.__getIndustryFromLocalFile()
        else:
            pass
        mydata=IndexCode.allIndex
        return mydata
    #----------------------------------------------------------------------
    @classmethod 
    def __getIndustryFromLocalFile(self):
        exists=os.path.isfile(IndexCode.localFileStr)
        if exists==True:
            f=h5py.File(IndexCode.localFileStr,'r')
            myKeys=list(f.keys())
            f.close()
            lastStoreDate=datetime.datetime.strptime(max(myKeys), "%Y%m%d")
            if (myKeys==[] or (datetime.datetime.now() - relativedelta(month=+3))>lastStoreDate):#如果3个月没有更新，重新抓取数据
                mydata=IndexCode.__getAllDataFromOracleServer()
            else:
                store = pd.HDFStore(IndexCode.localFileStr,'r')
                mydata=store.select(max(myKeys))
                store.close()
        else:
            mydata=IndexCode.__getAllDataFromOracleServer()
        return mydata
    
    #----------------------------------------------------------------------
    @classmethod 
    def __getAllDataFromOracleServer(self):
        oracleConnectString=OracleServer['default']
        oracleStr="select S_INFO_INDEXCODE as \"code\",S_INFO_NAME as \"name\",S_INFO_INDUSTRYCODE as code2,S_INFO_INDUSTRYNAME as name2 from wind_filesync.IndexContrastSector"
        connection = oracle.connect(oracleConnectString)
        cursor = connection.cursor()
        cursor.execute(oracleStr)
        mydata = cursor.fetchall()
        mydata = pd.DataFrame(mydata,columns=['code','name','code2','name2'])
        store = pd.HDFStore(IndexCode.localFileStr,'a')
        store.append(IndexCode.nowStr,mydata,append=False,format="table",data_columns=['code','name','code2','name2'])
        store.close()
        return mydata  
    #----------------------------------------------------------------------
########################################################################

