import pandas as pd
import os
from Config.myConfig import *
from DataAccess.TradedayDataProcess import TradedayDataProcess
from joblib import Parallel, delayed,parallel_backend
########################################################################
class machineLeariningBase(object):
    """机器学习基类"""
    #----------------------------------------------------------------------
    def __init__(self):
        pass
    #----------------------------------------------------------------------
    def getDataFromLocalDaily(self,path,today):
        fileName=os.path.join(path,str(today)+'.h5')
        data=pd.DataFrame()
        try:
            with pd.HDFStore(fileName,'r',complib='blosc:zstd',append=True,complevel=9) as store:
                data=store['data']
        except Exception as excp:
            logger.error(f'{fileName} error! {excp}')
            pass
        return data
    #----------------------------------------------------------------------
    def getDataFromLocal(self,path,startDate,endDate):
        tradedays=TradedayDataProcess.getTradedays(startDate,endDate)
        tradedays=list(tradedays)
        with parallel_backend("multiprocessing", n_jobs=MYJOBS):
            mydata=Parallel()(delayed(self.getDataFromLocalDaily)(path,tradedays[i]) for i in range(len(tradedays)))
        all=pd.concat(mydata)
        return all
        pass
########################################################################