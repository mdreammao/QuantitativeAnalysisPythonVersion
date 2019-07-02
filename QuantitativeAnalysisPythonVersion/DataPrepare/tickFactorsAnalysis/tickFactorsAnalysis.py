from DataAccess.IndexComponentDataProcess import *
from DataAccess.KLineDataProcess import *
from DataAccess.TradedayDataProcess import TradedayDataProcess
from DataPrepare.dailyFactorsProcess import dailyFactorsProcess
from DataPrepare.tickFactorsProcess import tickFactorsProcess
from DataAccess.TickDataProcess import *
from Utility.JobLibUtility import *
from Utility.TradeUtility import *
import warnings
from Config.myConstant import *
from Config.myConfig import *
import numpy as np
import os
import copy


########################################################################
class tickFactorsAnalysis(object):
    """分析tick因子"""
    #------------------------------------------------------------------
    def __init__(self,document):
        self.path=os.path.join(LocalFileAddress,document)
        pass
    #------------------------------------------------------------------
    def getDataFromLocalDaily(self,code,today):
        fileName=os.path.join(self.path,str(code).replace('.','_'),str(today)+'.h5')
        data=pd.DataFrame()
        try:
            with pd.HDFStore(fileName,'r',complib='blosc:zstd',append=True,complevel=9) as store:
                data=store['data']
        except Exception as excp:
            #logger.error(f'{fileName} error! {excp}')
            pass
        return data
    #------------------------------------------------------------------
    def getDataFromLocalByDays(self,code,days):
        data=[]
        for day in days:
            data.append(self.getDataFromLocalDaily(code,day))
        data=pd.concat(data)
        return data
    #------------------------------------------------------------------
    def getDataFromLocalByCodes(self,codes,day):
        data=[]
        for code in codes:
            data.append(self.getDataFromLocalDaily(code,day))
        data=pd.concat(data)
        return data
    #----------------------------------------------------------------------
    def useJobLibToGetFactorDataCodeByCode(self,tradedays,groupnum,code):
        warnings.filterwarnings('ignore')
        if groupnum>len(tradedays):
            groupnum=len(tradedays)
            pass
        days={i:[] for i in range(tradedays)}
        for i in range(0,len(groupnum)):
            mygroup=i%groupnum
            days[mygroup].append(tradedays[i])
        with parallel_backend("multiprocessing", n_jobs=JobLibUtility.myjobs):
            mydata=Parallel()(delayed(self.getDataFromLocalByDays)(code,list(days[i])) for i in range(groupnum))
        mydata=pd.concat(mydata)
        return mydata
    #----------------------------------------------------------------------
    def useJobLibToGetFactorDataDaily(self,stockCodes,groupnum,date):
        warnings.filterwarnings('ignore')
        if groupnum>len(stockCodes):
            groupnum=len(stockCodes)
            pass
        stocks={i:[] for i in range(groupnum)}
        for i in range(0,len(stockCodes)):
            mygroup=i%groupnum
            stocks[mygroup].append(stockCodes[i])
        with parallel_backend("multiprocessing", n_jobs=JobLibUtility.myjobs):
            mydata=Parallel()(delayed(self.getDataFromLocalByCodes)(list(stocks[i]),date) for i in range(groupnum))
        mydata=pd.concat(mydata)
        return mydata
    #----------------------------------------------------------------------
    def analysisPerCode(self,code,startDate,endDate,feature,target):
        tradedays=list(TradedayDataProcess.getTradedays(startDate,endDate))
        mydata=self.useJobLibToGetFactorDataCodeByCode(tradedays,100,code)
        mycorr=mydata[[feature,target]].corr()
        print(mydata.shape)
        return mycorr
        pass
    #----------------------------------------------------------------------
    def analysisDaily(self,codes,date,feature,target):
        mydata=self.useJobLibToGetFactorDataDaily(codes,400,date)
        mycorr=mydata[[feature,target]].corr()
        print(mycorr)
        return mycorr
        
########################################################################
