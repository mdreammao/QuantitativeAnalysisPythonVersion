from DataPrepare.dailyFactors.factorBase import factorBase 
from DataAccess.TradedayDataProcess import TradedayDataProcess
from DataAccess.KLineDataProcess import  KLineDataProcess
from Config.myConstant import *
from Config.myConfig import *
import pandas as pd
########################################################################
class closeStd(factorBase):
    """description of class"""

#----------------------------------------------------------------------
    def __init__(self):
        super().__init__()
        self.factor='closeStd'
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
        #需要前推100天来获取计算得数据
        startDate=TradedayDataProcess.getPreviousTradeday(startDate,100)
        [listDate,delistDate]=super().setIPODate(code)
        startDate=max(startDate,listDate)
        dailyData=KLineDataProcess('daily')
        mydata=dailyData.getDataByDate(code,startDate,endDate)
        if mydata.shape[0]==0:
            return pd.DataFrame()
        mydata['return']=(mydata['close']/mydata['preClose']-1)
        mydata.loc[mydata['status']=='停牌','return']=np.nan
        mydata.loc[mydata['date']==listDate,'return']=np.nan
        mydata['yesterdayReturn']=mydata['return'].shift(1)
        mydata['closeStd20']=mydata['yesterdayReturn'].rolling(20,min_periods=16).std()
        mydata['ts_closeStd20']=mydata['closeStd20'].rolling(50,min_periods=20).apply((lambda x:pd.Series(x).rank().iloc[-1]/len(x)),raw=True)
        mycolumns=['date','yesterdayReturn','closeStd20','ts_closeStd20']
        mydata=mydata[mycolumns]
        return mydata
########################################################################

