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
    localFileStr=os.path.join(LocalFileAddress,'indexCode.h5')
    allIndex=None
    #----------------------------------------------------------------------
    #返还指数信息，'code'指数代码,'name'指数名称,'code2'指数数据库内部代码,'name2'指数数据库内部名称
    @classmethod 
    def getIndexCodeInfo(self):
        if not IndexCode.allIndex:
            IndexCode.allIndex=IndexCode.__getIndexCodeFromLocalFile()
        else:
            pass
        mydata=IndexCode.allIndex
        return mydata
    #----------------------------------------------------------------------
    @classmethod 
    def updateIndexCodeFromLocalFile(self):
        logger.info(f'update index code list!')
        mydata=IndexCode.__getAllDataFromOracleServer()
    #----------------------------------------------------------------------
    @classmethod 
    def __getIndexCodeFromLocalFile(self):
        exists=os.path.isfile(IndexCode.localFileStr)
        if exists==True:
            store = pd.HDFStore(IndexCode.localFileStr,'r')
            mydata=store['data']
            store.close()
        else:
            logger.warning(f'There is no index code list data!')
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
        store = pd.HDFStore(IndexCode.localFileStr,'a',complib='blosc:zstd',append=True,complevel=9)
        store.append('data',mydata,append=False,format="table",data_columns=mydata.columns)
        store.close()
        return mydata  
    #----------------------------------------------------------------------
########################################################################

