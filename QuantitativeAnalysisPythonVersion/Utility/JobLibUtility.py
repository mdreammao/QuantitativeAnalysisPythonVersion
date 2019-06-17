import pandas as pd
import numpy as np
import datetime
#import numba
import warnings
from Config.myConstant import *
from Config.myConfig import *
from joblib import Parallel, delayed,parallel_backend
#from Utility.HDF5Utility import *

########################################################################
class JobLibUtility(object):
    """joblib并行化辅助程序"""
    myjobs=MYJOBS
    #----------------------------------------------------------------------
    def __init__(self):
        pass
    #----------------------------------------------------------------------
    @classmethod 
    def useJobLib(self,myfunction,stockCodes,groupnum,startDate,endDate,targetFilePath):
        warnings.filterwarnings('ignore')
        if groupnum>len(stockCodes):
            groupnum=len(stockCodes)
            pass
        stocks={i:[] for i in range(groupnum)}
        for i in range(0,len(stockCodes)):
            mygroup=i%groupnum
            stocks[mygroup].append(stockCodes[i])
        tmpAddress={}
        for i in range(groupnum):
            tmpAddress[i]=TempLocalFileAddress+"\\tmp{0}.h5".format(str(i))
            HDF5Utility.fileClear(tmpAddress[i])
        Parallel(n_jobs=JobLibUtility.myjobs)(delayed(myfunction)(list(stocks[i]),startDate,endDate,tmpAddress[i]) for i in range(groupnum))
        for i in range(groupnum):
            HDF5Utility.dataTransfer(tmpAddress[i],targetFilePath)
        pass
    #----------------------------------------------------------------------
    @classmethod 
    def useJobLibStoreToOneFile(self,myfunction,stockCodes,groupnum,startDate,endDate,targetFilePath):
        warnings.filterwarnings('ignore')
        if groupnum>len(stockCodes):
            groupnum=len(stockCodes)
            pass
        stocks={i:[] for i in range(groupnum)}
        for i in range(0,len(stockCodes)):
            mygroup=i%groupnum
            stocks[mygroup].append(stockCodes[i])
        tmpAddress={}
        for i in range(groupnum):
            tmpAddress[i]=TempLocalFileAddress+"\\tmp{0}.h5".format(str(i))
            HDF5Utility.fileClear(tmpAddress[i])
        Parallel(n_jobs=JobLibUtility.myjobs)(delayed(myfunction)(list(stocks[i]),startDate,endDate,tmpAddress[i]) for i in range(groupnum))
        for i in range(groupnum):
            HDF5Utility.dataTransferToOneFile(tmpAddress[i],targetFilePath)
        pass
    #----------------------------------------------------------------------
    @classmethod 
    def useJobLibToGetData(self,myfunction,stockCodes,groupnum,startDate,endDate):
        warnings.filterwarnings('ignore')
        if groupnum>len(stockCodes):
            groupnum=len(stockCodes)
            pass
        stocks={i:[] for i in range(groupnum)}
        allData=[]
        for i in range(0,len(stockCodes)):
            mygroup=i%groupnum
            stocks[mygroup].append(stockCodes[i])
        tmpAddress={}
        with parallel_backend("multiprocessing", n_jobs=JobLibUtility.myjobs):
            mydata=Parallel()(delayed(myfunction)(list(stocks[i]),startDate,endDate) for i in range(groupnum))
        for i in range(groupnum):
            allData.append(mydata[i])
        allData=pd.concat(allData)
        return allData
    #----------------------------------------------------------------------
    @classmethod 
    def useJobLibToGetFactorDataDaily(self,myfunction,stockCodes,groupnum,date,factors=TICKFACTORSUSED):
        warnings.filterwarnings('ignore')
        if groupnum>len(stockCodes):
            groupnum=len(stockCodes)
            pass
        stocks={i:[] for i in range(groupnum)}
        for i in range(0,len(stockCodes)):
            mygroup=i%groupnum
            stocks[mygroup].append(stockCodes[i])
        with parallel_backend("multiprocessing", n_jobs=JobLibUtility.myjobs):
            mydata=Parallel()(delayed(myfunction)(list(stocks[i]),date,factors) for i in range(groupnum))
        mydata=pd.concat(mydata)
        return mydata
    #----------------------------------------------------------------------
    @classmethod 
    def useJobLibToUpdateData(self,myfunction,stockCodes,groupnum,startDate,endDate):
        warnings.filterwarnings('ignore')
        if groupnum>len(stockCodes):
            groupnum=len(stockCodes)
            pass
        stocks={i:[] for i in range(groupnum)}
        allData=pd.DataFrame()
        for i in range(0,len(stockCodes)):
            mygroup=i%groupnum
            stocks[mygroup].append(stockCodes[i])
        tmpAddress={}
        with parallel_backend("multiprocessing", n_jobs=JobLibUtility.myjobs):
            mydata=Parallel()(delayed(myfunction)(list(stocks[i]),startDate,endDate) for i in range(groupnum))
    #----------------------------------------------------------------------
    @classmethod 
    def useJobLibToUpdateFacotrs(self,myfunction,stockCodes,groupnum,factors):
        warnings.filterwarnings('ignore')
        if groupnum>len(stockCodes):
            groupnum=len(stockCodes)
            pass
        stocks={i:[] for i in range(groupnum)}
        allData=pd.DataFrame()
        for i in range(0,len(stockCodes)):
            mygroup=i%groupnum
            stocks[mygroup].append(stockCodes[i])
        tmpAddress={}
        with parallel_backend("multiprocessing", n_jobs=JobLibUtility.myjobs):
            mydata=Parallel()(delayed(myfunction)(list(stocks[i]),factors) for i in range(groupnum))
        for i in range(groupnum):
            allData=allData.append(mydata[i])
        return allData
    #----------------------------------------------------------------------
    @classmethod 
    def useJobLibToComputeByCodes(self,myfunction,stockCodes,groupnum,startDate,endDate,parameters):
        warnings.filterwarnings('ignore')
        if groupnum>len(stockCodes):
            groupnum=len(stockCodes)
            pass
        stocks={i:[] for i in range(groupnum)}
        for i in range(0,len(stockCodes)):
            mygroup=i%groupnum
            stocks[mygroup].append(stockCodes[i])
        tmpAddress={}
        with parallel_backend("multiprocessing", n_jobs=JobLibUtility.myjobs):
            mydata=Parallel()(delayed(myfunction)(list(stocks[i]),startDate,endDate,parameters) for i in range(groupnum))
        allData=pd.concat(mydata)
        return allData
########################################################################
