from DataAccess.IndexComponentDataProcess import *
from DataAccess.KLineDataProcess import *
from DataAccess.TradedayDataProcess import *
from DataPrepare.dailyFactorsProcess import *
from DataPrepare.tickFactorsProcess import *
from DataAccess.TickDataProcess import *
from Utility.JobLibUtility import *
import warnings
from Config.myConstant import *
from Config.myConfig import *
import numpy as np
import os
########################################################################
class stockReverseByStdOnTick(object):
    """股票异动,专注股票大涨之后的回调"""
    #----------------------------------------------------------------------
    def __init__(self):
        self.__localFileStr=LocalFileAddress+"\\intermediateResult\\stdFeature.h5"
        self.__localFileStrResult=LocalFileAddress+"\\intermediateResult\\stdReverseResult.h5"
        self.__allMinute=pd.DataFrame()
        self.__key='factorsWithRank'
        self.__factorsAddress=LocalFileAddress+"\\{0}\\{1}.h5".format('dailyFactors',self.__key)
        pass
    
    #----------------------------------------------------------------------
    def reverse(self,code,startDate,endDate):
        days=list(TradedayDataProcess().getTradedays(startDate,endDate))
        factors=['closeStd','index','marketValue','industry']
        dailyRepo=dailyFactorsProcess()
        dailyFactor=dailyRepo.getSingleStockDailyFactors(code,factors,startDate,endDate)
        dailyKLine=KLineDataProcess('daily')
        dailyData=dailyKLine.getDataByDate(code,startDate,endDate)
        tick=TickDataProcess()
        for today in days:
            tickData=tick.getResampleTickShotData(code,today)
            todayInfo=dailyFactor[dailyFactor['date']==today]
            todayKLine=dailyData[dailyData['date']==today]
            pass
        pass

########################################################################