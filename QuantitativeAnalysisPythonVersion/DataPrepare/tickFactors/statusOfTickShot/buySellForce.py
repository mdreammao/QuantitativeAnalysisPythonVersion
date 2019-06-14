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
    #----------------------------------------------------------------------
    def getFactorFromLocalFile(self,code,date):
        mydata=super().getFromLocalFile(code,date,'buySellForce')
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
        result=self.__computerFactor(code,date,data)
        super().updateFactor(code,date,self.factor,result)
    #----------------------------------------------------------------------
    def __buySellWeightedVolumeRatio(self,data,n):
        data['buyWeightedVolume'+str(n)]=0
        data['sellWeightedVolume'+str(n)]=0
        for i in range(n):
            data['buyWeightedVolume'+str(n)]=data['buyWeightedVolume'+str(n)]+data['BV'+str(i)]*(mydata['midPrice']-data['B'+str(i)])
            data['sellWeightedVolume'+str(n)]=data['sellWeightedVolume'+str(n)]+data['SV'+str(i)]*(-mydata['midPrice']+data['S'+str(i)])
            pass
        data['buySellWeightedVolumeRatio'+str(n)]=data['buyWeightedVolume'+str(n)]/(data['buyWeightedVolume'+str(n)]+data['sellWeightedVolume'+str(n)])
        pass
    #----------------------------------------------------------------------
    def __computerFactor(self,code,date,mydata):
        result=pd.DataFrame()
        if mydata.shape[0]!=0:
            #index对齐即可
            result=pd.DataFrame(index=mydata.index)
            #------------------------------------------------------------------
            #bid ask 间距，因子值在[0,0.1]之间
            result['buySellSpread']=0.01
            select=(mydata['S1']!=0) & (mydata['B1']!=0)
            result.loc[select,'buySellSpread']=((mydata['B1']-mydata['S1'])/mydata['midPrice'])[select]
            #------------------------------------------------------------------
            #买卖盘口静态信息,因子值为正整数
            result['buyVolume2']=mydata['BV1']+mydata['BV2']
            result['sellVolume2']=mydata['SV1']+mydata['SV2']
            result['buyVolume5']=(mydata['BV1']+mydata['BV2']+mydata['BV3']+mydata['BV4']+mydata['BV5'])
            result['sellVolume5']=(mydata['SV1']+mydata['SV2']+mydata['SV3']+mydata['SV4']+mydata['SV5'])
            result['buyVolume10']=(mydata['BV1']+mydata['BV2']+mydata['BV3']+mydata['BV4']+mydata['BV5']+mydata['BV6']+mydata['BV7']+mydata['BV8']+mydata['BV9']+mydata['BV10'])
            result['sellVolume10']=(mydata['SV1']+mydata['SV2']+mydata['SV3']+mydata['SV4']+mydata['SV5']+mydata['SV6']+mydata['SV7']+mydata['SV8']+mydata['SV9']+mydata['SV10'])
            result['totalVolume10']=result['buyVolume10']+result['sellVolume10']
            #------------------------------------------------------------------
            #挂单量比
            result['buySellVolumeRatio2']=(result['buyVolume2']/(result['sellVolume2']+result['buyVolume2']))
            result['buySellVolumeRatio5']=(result['buyVolume5']/(result['sellVolume5']+result['buyVolume5']))
            result['buySellVolumeRatio10']=(result['buyVolume10']/(result['sellVolume10']+result['buyVolume10']))
            #------------------------------------------------------------------
            #加权之后的多空力量对比
            #根据价格和量计算的多空力量对比
            self.__buySellWeightedVolumeRatio(result,2)
            self.__buySellWeightedVolumeRatio(result,5)
            self.__buySellWeightedVolumeRatio(result,10)
            #------------------------------------------------------------------
            #计算盘口变动,只有买卖盘价格变化的时候才计算
            #计算买方力量和卖方力量
            S=['S1','S2','S3','S4','S5','S6','S7','S8','S9','S10']
            SV=['SV1','SV2','SV3','SV4','SV5','SV6','SV7','SV8','SV9','SV10']
            B=['B1','B2','B3','B4','B5','B6','B7','B8','B9','B10']
            BV=['BV1','BV2','BV3','BV4','BV5','BV6','BV7','BV8','BV9','BV10']
            #------------------------------------------------------------------
            result['buyForceIncrease']=0
            result['sellForceIncrease']=0
            for i in range(len(B)):
                price=B[i]
                volume=BV[i]
                select=(mydata['B1']<mydata[price].shift(1)) & (mydata[price].shift(1)!=0) & (mydata['B1']!=mydata['S1'])
                result.loc[select,'buyForceIncrease']=(result['buyForceIncrease']-mydata[volume].shift(1))[select]
            select=(mydata['B1']>=mydata['B1'].shift(1)) & (mydata['B1']!=mydata['S1'])
            result.loc[select,'buyForceIncrease']=mydata['BV1'][select]
            select=(mydata['B1']==mydata['B1'].shift(1)) & (mydata['B1']!=mydata['S1'])
            result.loc[select,'buyForceIncrease']=(result['buyForceIncrease']-mydata['BV1'].shift(1))[select]
            for i in range(len(S)):
                price=S[i]
                volume=SV[i]
                select=(mydata['B1']>=mydata[price].shift(1)) & (mydata[price].shift(1)!=0) & (mydata['B1']!=mydata['S1'])
                result.loc[select,'buyForceIncrease']=(result['buyForceIncrease']+mydata[volume].shift(1))[select]
            select=result['sellVolume10'].shift(1)>=0
            result.loc[select,'buyForceIncrease']=(result['buyForceIncrease']/result['sellVolume10'].shift(1))[select]
            #------------------------------------------------------------------
            for i in range(len(S)):
                price=S[i]
                volume=SV[i]
                select=(mydata['S1']>mydata[price].shift(1)) & (mydata[price].shift(1)!=0) & (mydata['B1']!=mydata['S1'])
                result.loc[select,'sellForceIncrease']=(result['sellForceIncrease']-mydata[volume].shift(1))[select]
            select=(mydata['S1']<=mydata['S1'].shift(1)) & (mydata['B1']!=mydata['S1'])
            result.loc[select,'sellForceIncrease']=mydata['SV1'][select]
            select=(mydata['S1']==mydata['S1'].shift(1)) & (mydata['B1']!=mydata['S1'])
            result.loc[select,'sellForceIncrease']=(result['sellForceIncrease']-mydata['SV1'].shift(1))[select]
            for i in range(len(B)):
                price=B[i]
                volume=BV[i]
                select=(mydata['S1']<=mydata[price].shift(1)) & (mydata[price].shift(1)!=0) & (mydata['B1']!=mydata['S1'])
                result.loc[select,'sellForceIncrease']=(result['sellForceIncrease']+mydata[volume].shift(1))[select]
            select=result['buyVolume10'].shift(1)>0
            result.loc[select,'sellForceIncrease']=(result['sellForceIncrease']/result['buyVolume10'].shift(1))[select]
            #------------------------------------------------------------------
            #多空力量变化
            select=((result['buyForceIncrease'].isna()) & (~result['sellForceIncrease'].isna()))
            result.loc[select,'buyForceIncrease']=0
            select=((result['sellForceIncrease'].isna()) & (~result['buyForceIncrease'].isna()))
            result.loc[select,'sellForceIncrease']=0
            result['buySellForceChange']=result['buyForceIncrease']-result['sellForceIncrease']
            pass
        else:
            logger.error(f'There no data of {code} in {date} to computer factor!') 
        return result
########################################################################
