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
        for i in range(1,n+1):
            select=data['B'+str(i)]>0
            data.loc[select,'buyWeightedVolume'+str(n)]=data['buyWeightedVolume'+str(n)]+data['BV'+str(i)]*(data['buySellSpread']*data['midPrice']/(data['midPrice']-data['B'+str(i)]))[select]
            select=data['S'+str(i)]>0
            data.loc[select,'sellWeightedVolume'+str(n)]=data['sellWeightedVolume'+str(n)]+data['SV'+str(i)]*(data['buySellSpread']*data['midPrice']/(-data['midPrice']+data['S'+str(i)]))[select]
            pass
        select=data['B1']==data['S1']
        data['buySellWeightedVolumeRatio'+str(n)]=data['buyWeightedVolume'+str(n)]/(data['buyWeightedVolume'+str(n)]+data['sellWeightedVolume'+str(n)])
        data.loc[select,'buySellWeightedVolumeRatio'+str(n)]=(data['BV1']/(data['BV1']+data['SV1']))[select]
        pass
    #----------------------------------------------------------------------
    def __computerFactor(self,code,date,mydata):
        result=pd.DataFrame()
        if mydata.shape[0]!=0:
            #index对齐即可
            result=mydata.copy()
            #------------------------------------------------------------------
            #bid ask 间距，因子值在[0,0.1]之间
            result['buySellSpread']=0.01
            select=(mydata['S1']!=0) & (mydata['B1']!=0)
            result.loc[select,'buySellSpread']=((mydata['S1']-mydata['B1'])/mydata['midPrice'])[select]
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
            #根据价格和量计算的多空力量对比因子值在[0,1]之间
            self.__buySellWeightedVolumeRatio(result,2)
            self.__buySellWeightedVolumeRatio(result,5)
            self.__buySellWeightedVolumeRatio(result,10)
            #------------------------------------------------------------------
            #主动买和主动卖，因子值大小在[0,+∞)
            select0=mydata['S1']!=mydata['B1']
            result['buyForce']=0
            for i in range(1,11):
                select=(mydata['S1']>mydata['S'+str(i)].shift(1)) & select0
                result.loc[select,'buyForce']=(result['buyForce']+mydata['SV'+str(i)].shift(1)/result['buyVolume10'].shift(1))[select]
                select=(mydata['S1']==mydata['S'+str(i)].shift(1)) & (mydata['SV'+str(i)].shift(1)>mydata['SV1']) & select0
                result.loc[select,'buyForce']=(result['buyForce']+(mydata['SV'+str(i)].shift(1)-mydata['SV1'])/result['buyVolume10'].shift(1))[select]
                pass
            result['sellForce']=0
            for i in range(1,11):
                select=(mydata['B1']<mydata['B'+str(i)].shift(1)) & select0
                result.loc[select,'sellForce']=(result['sellForce']+mydata['BV'+str(i)].shift(1)/result['sellVolume10'].shift(1))[select]
                select=(mydata['B1']==mydata['B'+str(i)].shift(1)) & (mydata['BV'+str(i)].shift(1)>mydata['BV1']) & select0
                result.loc[select,'sellForce']=(result['sellForce']+(mydata['BV'+str(i)].shift(1)-mydata['BV1'])/result['sellVolume10'].shift(1))[select]
                pass
            result['buySellForceChange']=result['buyForce']-result['sellForce']
            #------------------------------------------------------------------
            #剔除14点57分之后，集合竞价的数据
            result=result[result['time']<'145700000']
            mycolumns=list(set(result.columns).difference(set(mydata.columns)))
            result=result[mycolumns]
            super().checkDataNan(code,date,self.factor,result)
            pass
        else:
            logger.error(f'There no data of {code} in {date} to computer factor!') 
        return result
########################################################################
