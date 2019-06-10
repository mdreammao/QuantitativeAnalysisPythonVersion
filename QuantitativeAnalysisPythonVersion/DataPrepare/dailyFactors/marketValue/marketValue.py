from DataPrepare.dailyFactors.factorBase import factorBase 
from DataAccess.TradedayDataProcess import TradedayDataProcess
from DataAccess.KLineDataProcess import  KLineDataProcess
from Config.myConstant import *
from Config.myConfig import *
import pandas as pd
########################################################################
class marketValue(factorBase):
    """description of class"""

#----------------------------------------------------------------------
    def __init__(self):
        super().__init__()
        self.factor='marketValue'
        pass
    #----------------------------------------------------------------------
    def updateFactor(self,code):
        exists=super().checkLocalFile(code,self.factor)
        [startDate,endDate]=super().getNeedToUpdateDaysOfFactor(code,self.factor)
        if endDate<startDate:#无需更新
            return 
        result=self.__computerFactor(code,startDate,endDate)
        super().updateFactor(code,self.factor,result)
    #----------------------------------------------------------------------
    #给定原始数据和日期进行计算
    def __computerFactor(self,code,startDate,endDate):  
        tradedays=TradedayDataProcess.getTradedays(startDate,endDate)
        mydata=pd.DataFrame(data=tradedays)
        mydata.set_index('date',drop=True,inplace=True)
        myDailyDerivative=KLineDataProcess('dailyDerivative')
        mydataDerivative=myDailyDerivative.getDataByDate(code,startDate,endDate)
        if mydataDerivative.shape[0]==0:
            return pd.DataFrame()
            pass
        mydataDerivative.set_index('date',inplace=True)
        mydata['freeShares']=mydataDerivative['freeShares']
        mydata['freeMarketValue']=mydataDerivative['freeMarketValue']
        mydata.reset_index(drop=False,inplace=True)
        mycolumns=['date','freeShares','freeMarketValue']
        mydata=mydata[mycolumns]
        return mydata
########################################################################

