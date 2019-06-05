from Config.myConstant import *
from Config.myConfig import *
from DataPrepare.tickFactors.factorBase import factorBase
from DataAccess.TickDataProcess import TickDataProcess
import pandas as pd
import math
########################################################################
class midPriceChange(factorBase):
    """描述盘口状态的因子"""
    #----------------------------------------------------------------------
    def __init__(self):
        #super(buySellVolumeRatio,self).__init__()
        super().__init__()
        self.factor='midPriceChange'
        pass
    def getFactorFromLocalFile(self,code,date):
        mydata=super().getFromLocalFile(code,date,'midPriceChange')
        return mydata
        pass
    def updateFactor(self,code,date,data=pd.DataFrame()):
        exists=super().checkLocalFile(code,date,self.factor)
        if exists==True:
            logger.info(f'No need to compute! {self.factor} of {code} in {date} exists!')
            pass
        if data.shape[0]==0:
             data=TickDataProcess().getDataByDateFromLocalFile(code,date)
        result=self.__computerFactor(code,date,data)
        super().updateFactor(code,date,self.factor,result)

    def __computerFactor(self,code,date,mydata):
        result=pd.DataFrame()
        if mydata.shape[0]!=0:
            #result['midIncreaseNext1m']=mydata['midPrice'].shift(-20)/mydata['midPrice']-1
            #result['midIncreaseNext5m']=mydata['midPrice'].shift(-100)/mydata['midPrice']-1
            #result['midIncreaseNext10m']=mydata['midPrice'].shift(-200)/mydata['midPrice']-1
            #计算指标，根据前3分钟的数据计算
            result=pd.DataFrame(index=mydata.index)
            result['midIncreasePrevious3m']=mydata['midPrice']/mydata['midPrice'].shift(60)-1
            result['differenceHighLow']=mydata['midPrice'].rolling(50,min_periods=20).max()/mydata['midPrice'].rolling(50,min_periods=20).min()-1
            result['vwap3m']= (mydata['amount']-mydata['amount'].shift(60))/(mydata['volume']-mydata['volume'].shift(60))
            result['differenceMidVwap']=mydata['midPrice']- result['vwap3m']
            result['midStd60']=mydata['midPrice'].rolling(60,min_periods=20).std()*math.sqrt(14400/3)
            #计算指标的ts值,按50个数据计算
            mycolumns=['midIncreasePrevious3m','differenceHighLow','vwap3m','differenceMidVwap','midStd60']
            #mycolumns=[]
            for col in mycolumns:
                result['ts_'+col]=result[col].rolling(50,min_periods=20).apply((lambda x:pd.Series(x).rank().iloc[-1]/len(x)),raw=True)
            pass
        else:
            logger.error(f'There no data of {code} in {date} to computer factor!') 
        return result
########################################################################
