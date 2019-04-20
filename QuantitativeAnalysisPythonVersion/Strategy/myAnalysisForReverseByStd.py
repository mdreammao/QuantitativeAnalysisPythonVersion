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
        self.__localFileStrResult=LocalFileAddress+"\\intermediateResult\\stdReverseResult.h5"
        pass
    #----------------------------------------------------------------------
    def analysis(self,startDate,endDate):
        startDate=str(startDate)
        endDate=str(endDate)
        self.startDate=startDate
        self.endDate=endDate
        self.tradeDays=TradedayDataProcess.getTradedays(startDate,endDate)
        mydata=pd.DataFrame()
        store = pd.HDFStore(self.__localFileStrResult,'a')
        keys=store.keys()
        for code in keys:
            mycode=code.lstrip("/")
            #print(mycode)
            mydata=mydata.append(store.get(mycode))
        store.close()
        print(mydata.shape)

     


########################################################################
