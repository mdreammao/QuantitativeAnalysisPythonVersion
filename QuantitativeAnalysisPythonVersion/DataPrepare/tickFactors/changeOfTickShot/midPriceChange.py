from Config.myConstant import *
from Config.myConfig import *
from DataPrepare.tickFactors.factorBase import factorBase
from DataAccess.TickDataProcess import TickDataProcess
import pandas as pd
import numpy as np
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
            result=mydata[['midPrice','amount','volume']].copy()
            
            #计算mid价格的涨跌
            result['midPriceIncrease']=(result['midPrice']-result['midPrice'].shift(1))/result['midPrice'].shift(1)
            select=[result['midPriceIncrease'].isna()==True]
            result.loc[select,'midPriceIncrease']=0
            result['midPrice3mIncrease']=result['midPrice'].rolling(60,min_periods=1).apply(lambda x:x[-1]/x[0]-1,raw=True)
            #计算今日开盘以来的vwap价格
            result['vwapToday']=result['amount']/result['volume']
            #如果vwap价格不存在，使用midPrice来代替
            select=[result['vwapToday'].isna()==True]
            result.loc[select,'vwapToday']=result.loc[select,'midPrice']
            #计算midprice到vwap价格的距离，大多数落在±10%
            result['midToVwap']=(result['midPrice']-result['vwapToday'])/result['vwapToday']
            #计算前推3分钟vvwap
            result['vwap3m']= (result['amount']-result['amount'].shift(60))/(result['volume']-result['volume'].shift(60))
            result.iloc[0:60,'vwap3m']=result.iloc[0:60,'vwapToday']
            select=[result['vwap3m'].isna()==True]
            result.loc[select,'vwap3m']=result.loc[select,'midPrice']
            #计算midprice到vwap3m价格的距离，大多数落在±10%
            result['midToVwap3m']=(result['midPrice']-result['vwap3m'])/result['vwap3m']
            #计算midprice的3m有界变差,大多数落在[0,1]之间
            result['midPriceBV3m']=result['midPriceIncrease'].rolling(60,min_periods=1).apply(lambda x:np.sum(np.abs(x)),raw=True)
            result['midIncreaseToBV3m']=result['midPrice3mIncrease']/result['midPriceBV3m']
            select=[result['midIncreaseToBV3m'].isna()==True]
            result.loc[select,'midIncreaseToBV3m']=0
            #前推3分钟的最大最小值之差
            result['maxMidPrice3m']=result['midPrice'].rolling(60,min_periods=1).max()
            result['minMidPrice3m']=result['midPrice'].rolling(60,min_periods=1).min()
            result['differenceHighLow3m']=(result['maxMidPrice3m']-result['minMidPrice3m'])/result['midPrice']
            result['midInPrevious3m']=(result['maxMidPrice3m']-result['midPrice'])/(result['maxMidPrice3m']-result['minMidPrice3m'])
            #前推前3mMidPrice的波动率
            result['midStd60']=result['midPriceIncrease'].rolling(60,min_periods=1).std()*math.sqrt(14400/3)
            result=result[~['midPrice','amount','volume']]
            super().checkDataNan(code,date,self.factor,result)
        else:
            logger.error(f'There no data of {code} in {date} to computer factor!') 
        return result
########################################################################
