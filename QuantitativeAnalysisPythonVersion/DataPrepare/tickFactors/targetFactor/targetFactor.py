from Config.myConstant import *
from Config.myConfig import *
from DataPrepare.tickFactors.factorBase import factorBase
from DataAccess.TickDataProcess import TickDataProcess
import pandas as pd
import numpy as np
########################################################################
class targetFactor(factorBase):
    """描述训练目标的函数"""
    #----------------------------------------------------------------------
    def __init__(self):
        #super(buySellVolumeRatio,self).__init__()
        super().__init__()
        self.factor='targetFactor'
        pass
    #----------------------------------------------------------------------
    def getFactorFromLocalFile(self,code,date):
        mydata=super().getFromLocalFile(code,date,'targetFactor')
        return mydata
        pass
    #----------------------------------------------------------------------
    def updateFactor(self,code,date,data=pd.DataFrame()):
        exists=super().checkLocalFile(code,date,self.factor)
        if exists==True:
            logger.info(f'No need to compute! {self.factor} of {code} in {date} exists!')
            pass
        if data.shape[0]==0:
             data=TickDataProcess().getDataByDateFromLocalFile(code,date)
        result=self.computerFactor(code,date,data)
        super().updateFactor(code,date,self.factor,result)
    #----------------------------------------------------------------------
    def computerFactor(self,code,date,mydata):
        result=pd.DataFrame()
        if mydata.shape[0]!=0:
            #index对齐即可
            result=mydata[['midPrice','time']].copy()
            result['midPrice'].fillna(method='ffill',inplace=True)
            #mid价格的增长率 1m 2m 5m
            result['midIncreaseNext1m']=mydata['midPrice'].shift(-20)/mydata['midPrice']-1
            result['midIncreaseNext2m']=mydata['midPrice'].shift(-40)/mydata['midPrice']-1
            result['midIncreaseNext5m']=mydata['midPrice'].shift(-100)/mydata['midPrice']-1
            #mid增长率的最大最小值
            result['midIncreaseMaxNext1m']=mydata['midPrice'].rolling(20).max().shift(-20)/mydata['midPrice']-1
            result['midIncreaseMinNext1m']=mydata['midPrice'].rolling(20).min().shift(-20)/mydata['midPrice']-1
            result['midIncreaseMaxNext2m']=mydata['midPrice'].rolling(40).max().shift(-40)/mydata['midPrice']-1
            result['midIncreaseMinNext2m']=mydata['midPrice'].rolling(40).min().shift(-40)/mydata['midPrice']-1
            result['midIncreaseMaxNext5m']=mydata['midPrice'].rolling(100).max().shift(-100)/mydata['midPrice']-1
            result['midIncreaseMinNext5m']=mydata['midPrice'].rolling(100).min().shift(-100)/mydata['midPrice']-1
            #------------------------------------------------------------------
            #剔除14点57分之后，集合竞价的数据
            result=result[result['time']<'145700000']
            mycolumns=list(set(result.columns).difference(set(mydata.columns)))
            mycolumns.sort()
            result=result[mycolumns]
            #super().checkDataNan(code,date,self.factor,result)
            pass
        else:
            logger.error(f'There no data of {code} in {date} to computer factor!') 
        return result
########################################################################
