from Config.myConstant import *
from Config.myConfig import *
from DataPrepare.tickFactors.factorBase import factorBase
from DataAccess.TickDataProcess import TickDataProcess
import pandas as pd
import numpy as np
########################################################################
class buySellForce(factorBase):
    """描述盘口状态的因子"""
    #----------------------------------------------------------------------
    def __init__(self):
        #super(buySellVolumeRatio,self).__init__()
        super().__init__()
        self.factor='buySellForce'
        pass
    def getFactorFromLocalFile(self,code,date):
        mydata=super().getFromLocalFile(code,date,'buySellForce')
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
           
            #index对齐即可
            result=pd.DataFrame(index=mydata.index)
            #mid价格的增长率
            result['midIncreaseNext1m']=mydata['midPrice'].shift(-20)/mydata['midPrice']-1
            result['midIncreaseNext5m']=mydata['midPrice'].shift(-100)/mydata['midPrice']-1
            result['midIncreaseNext10m']=mydata['midPrice'].shift(-200)/mydata['midPrice']-1
            result['midIncreaseNext20m']=mydata['midPrice'].shift(-400)/mydata['midPrice']-1
            #对手价增长率,计算买对价和卖对价的增长率
            select=(mydata['S1']!=0) & (mydata['B1'].shift(-20)!=0)
            result.loc[select,'buyIncreaseNext1m']=mydata[select]['B1'].shift(-20)/mydata[select]['S1']-1
            select=(mydata['B1']!=0) & (mydata['S1'].shift(-20)!=0)
            result.loc[select,'sellIncreaseNext1m']=mydata[select]['S1'].shift(-20)/mydata[select]['B1']-1
            #买卖盘口静态信息
            result['buyVolume2']=mydata['BV1']+mydata['BV2']
            result['sellVolume2']=mydata['SV1']+mydata['SV2']
            result['buyVolume5']=(mydata['BV1']+mydata['BV2']+mydata['BV3']+mydata['BV4']+mydata['BV5'])
            result['sellVolume5']=(mydata['SV1']+mydata['SV2']+mydata['SV3']+mydata['SV4']+mydata['SV5'])
            result['buyVolume10']=(mydata['BV1']+mydata['BV2']+mydata['BV3']+mydata['BV4']+mydata['BV5']+mydata['BV6']+mydata['BV7']+mydata['BV8']+mydata['BV9']+mydata['BV10'])
            result['sellVolume10']=(mydata['SV1']+mydata['SV2']+mydata['SV3']+mydata['SV4']+mydata['SV5']+mydata['SV6']+mydata['SV7']+mydata['SV8']+mydata['SV9']+mydata['SV10'])
            #计算盘口变动,只有买卖盘价格变化的时候才计算
            #计算买方力量
            S=['S1','S2','S3','S4','S5','S6','S7','S8','S9','S10']
            SV=['SV1','SV2','SV3','SV4','SV5','SV6','SV7','SV8','SV9','SV10']
            select=(mydata['B1']>=mydata['B1'].shift(1)) & (mydata['B1']!=mydata['S1'])
            result.loc[select,'buyForceIncrease']=mydata['BV1'][select]
            select=(mydata['B1']==mydata['B1'].shift(1)) & (mydata['B1']!=mydata['S1'])
            result.loc[select,'buyForceIncrease']=(result['buyForceIncrease']-mydata['BV1'].shift(1))[select]
            for i in range(len(S)):
                price=S[i]
                volume=SV[i]
                select=(mydata['B1']>=mydata[price].shift(1)) & (mydata[price].shift(1)!=0) & (mydata['B1']!=mydata['S1'])
                result.loc[select,'buyForceIncrease']=(result['buyForceIncrease']+mydata[volume].shift(1))[select]
            result['buyForceIncrease']=result['buyForceIncrease']/result['sellVolume10'].shift(1)
            #计算卖方力量
            B=['B1','B2','B3','B4','B5','B6','B7','B8','B9','B10']
            BV=['BV1','BV2','BV3','BV4','BV5','BV6','BV7','BV8','BV9','BV10']
            select=(mydata['S1']<=mydata['S1'].shift(1)) & (mydata['B1']!=mydata['S1'])
            result.loc[select,'sellForceIncrease']=mydata['SV1'][select]
            select=(mydata['S1']==mydata['S1'].shift(1)) & (mydata['B1']!=mydata['S1'])
            result.loc[select,'sellForceIncrease']=(result['sellForceIncrease']-mydata['SV1'].shift(1))[select]
            for i in range(len(S)):
                price=B[i]
                volume=BV[i]
                select=(mydata['S1']<=mydata[price].shift(1)) & (mydata[price].shift(1)!=0) & (mydata['B1']!=mydata['S1'])
                result.loc[select,'sellForceIncrease']=(result['sellForceIncrease']+mydata[volume].shift(1))[select]
            result['sellForceIncrease']=result['sellForceIncrease']/result['buyVolume10'].shift(1)
            #多空力量变化
            select=((result['buyForceIncrease']==np.nan) & (result['sellForceIncrease']!=np.nan))
            result.loc[select,'buyForceIncrease']=0
            select=((result['sellForceIncrease']==np.nan) & (result['buyForceIncrease']!=np.nan))
            result.loc[select,'sellForceIncrease']=0
            result['buySellForceChange']=result['buyForceIncrease']-result['sellForceIncrease']
            #挂单量比
            result['buySellVolumeRatio2']=((result['buyVolume2']/(result['sellVolume2']+result['buyVolume2']))-0.5)*2
            result['buySellVolumeRatio5']=((result['buyVolume5']/(result['sellVolume5']+result['buyVolume5']))-0.5)*2
            result['buySellVolumeRatio10']=((result['buyVolume10']/(result['sellVolume10']+result['buyVolume10']))-0.5)*2
            #result.loc[result['buySellVolumeRatio10']==np.inf,['buySellVolumeRatio1','buySellVolumeRatio5','buySellVolumeRatio10']]=np.nan
            #result.loc['buySellVolumeRatio1','buySellVolumeRatio5','buySellVolumeRatio10'].fillna(method='ffill',inplace=True)
            #result['buySellVolumeRatio2EMA']=result['buySellVolumeRatio2'].ewm(alpha=0.9,ignore_na=False,adjust=True).mean()
            #result['buySellVolumeRatio5EMA']=result['buySellVolumeRatio5'].ewm(alpha=0.9,ignore_na=False,adjust=True).mean()
            #result['buySellVolumeRatio10EMA']=result['buySellVolumeRatio10'].ewm(alpha=0.9,ignore_na=False,adjust=True).mean()
            
            #根据价格和量计算的多空力量对比
            result['buyPrice2']=(mydata['BV1']*mydata['B1']+mydata['BV2']*mydata['B2'])/result['buyVolume2']
            result['sellPrice2']=(mydata['SV1']*mydata['S1']+mydata['SV2']*mydata['S2'])/result['sellVolume2']
            result['buySellPriceRatio2']=(result['sellPrice2']-mydata['S1'])/(result['sellPrice2']-mydata['S1']+mydata['B1']-result['buyPrice2'])
            result['buyPrice5']=(mydata['BV1']*mydata['B1']+mydata['BV2']*mydata['B2']+mydata['BV3']*mydata['BV3']+mydata['BV4']*mydata['BV4']+mydata['BV5']*mydata['BV5'])/result['buyVolume5']
            result['sellPrice5']=(mydata['SV1']*mydata['S1']+mydata['SV2']*mydata['S2']+mydata['SV3']*mydata['SV3']+mydata['SV4']*mydata['SV4']+mydata['SV5']*mydata['SV5'])/result['sellVolume5']
            result['buySellPriceRatio5']=(result['sellPrice5']-mydata['S1'])/(result['sellPrice5']-mydata['S1']+mydata['B1']-result['buyPrice5'])
            #因子在前50条数据的分位数
            result['ts_volume1']=(mydata['BV1']+mydata['SV1']).rolling(50,min_periods=20).apply((lambda x:pd.Series(x).rank().iloc[-1]/len(x)),raw=True)
            result['ts_volume2']=(result['buyVolume2']+result['sellVolume2']).rolling(50,min_periods=20).apply((lambda x:pd.Series(x).rank().iloc[-1]/len(x)),raw=True)
            result['ts_volume5']=(result['buyVolume5']+result['sellVolume5']).rolling(50,min_periods=20).apply((lambda x:pd.Series(x).rank().iloc[-1]/len(x)),raw=True)
            mycolumns=['buySellVolumeRatio2','buySellVolumeRatio5','buySellVolumeRatio10','buySellPriceRatio2','buySellPriceRatio5','buyForceIncrease','sellForceIncrease','buySellForceChange']
            for col in mycolumns:
                result['ts_'+col]=result[col].rolling(50,min_periods=20).apply((lambda x:pd.Series(x).rank().iloc[-1]/len(x)),raw=True)
            #result['ts_buySellVolumeRatio2']=result['buySellVolumeRatio2'].rolling(50,min_periods=20).apply((lambda x:pd.Series(x).rank().iloc[-1]/len(x)),raw=True)
            #result['ts_buySellVolumeRatio5']=result['buySellVolumeRatio5'].rolling(50,min_periods=20).apply((lambda x:pd.Series(x).rank().iloc[-1]/len(x)),raw=True)
            #result['ts_buySellPriceRatio2']=result['buySellPriceRatio2'].rolling(50,min_periods=20).apply((lambda x:pd.Series(x).rank().iloc[-1]/len(x)),raw=True)
            #result['ts_buySellPriceRatio5']=result['buySellPriceRatio5'].rolling(50,min_periods=20).apply((lambda x:pd.Series(x).rank().iloc[-1]/len(x)),raw=True)
            pass
        else:
            logger.error(f'There no data of {code} in {date} to computer factor!') 
        return result
########################################################################
