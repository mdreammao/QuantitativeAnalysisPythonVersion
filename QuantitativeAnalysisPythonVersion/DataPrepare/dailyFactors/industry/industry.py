from DataPrepare.dailyFactors.factorBase import factorBase 
from DataAccess.TradedayDataProcess import TradedayDataProcess
from DataAccess.IndustryClassification import IndustryClassification
from Config.myConstant import *
from Config.myConfig import *
import pandas as pd
########################################################################
class industry(factorBase):
    """description of class"""

#----------------------------------------------------------------------
    def __init__(self):
        super().__init__()
        self.factor='industry'
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
        myindustry=IndustryClassification.getIndustryByCode(code,startDate,endDate)
        if myindustry.shape[0]==0:
            return pd.DataFrame()
        mydata['industry']=myindustry['industry']
        mydata['industryName']=myindustry['name']
        mydata.reset_index(drop=False,inplace=True)
        mycolumns=['date','industry','industryName']
        mydata=mydata[mycolumns]
        return mydata
        pass
########################################################################

