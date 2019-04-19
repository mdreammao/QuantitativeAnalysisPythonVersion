from DataAccess.IndexComponentDataProcess import *
from DataAccess.KLineDataProcess import *
from DataAccess.TradedayDataProcess import *
from Config.myConstant import *
from Config.myConfig import *


########################################################################
class myAnalysisForReverseByStd(object):
    """股票异动,专注股票大涨之后的回调"""
    #----------------------------------------------------------------------
    def __init__(self):
        self.__localFileStr=LocalFileAddress+"\\intermediateResult\\stdFeature.h5"
        self.__localFileStrResult=LocalFileAddress+"\\intermediateResult\\stdFeatureResult.h5"
        self.__allMinute=pd.DataFrame()
        self.__key='factors'
        self.__factorsAddress=LocalFileAddress+"\\{0}\\{1}.h5".format('dailyFactors',self.__key)
        pass
    #----------------------------------------------------------------------
    def select(self,startDate,endDate):
        startDate=str(startDate)
        endDate=str(endDate)
        self.startDate=startDate
        self.endDate=endDate
        self.tradeDays=TradedayDataProcess.getTradedays(startDate,endDate)
        store = pd.HDFStore(self.__localFileStr,'a')
        stockCodes=store.select('stockCodes')
        data=store.select('result')
        store.close()
        pass
    #----------------------------------------------------------------------
    def analysis(self,startDate,endDate):
        startDate=str(startDate)
        endDate=str(endDate)
        self.startDate=startDate
        self.endDate=endDate
        self.tradeDays=TradedayDataProcess.getTradedays(startDate,endDate)
        store = pd.HDFStore(self.__localFileStrResult,'a')
        stockCodes=store.select('stockCodes')
        data=store.select('result')
        store.close()
     


########################################################################
