from Config.myConstant import *
from Config.myConfig import *
from DataPrepare.tickFactors.factorBase import factorBase
from DataAccess.TickDataProcess import TickDataProcess
import pandas as pd
import numpy as np
import math
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
             #data=TickDataProcess().getDataByDateFromLocalFile(code,date)
             data=TickDataProcess().getTickShotDataFromInfluxdbServer(code,date)
        result=self.computerFactor(code,date,data)
        super().updateFactor(code,date,self.factor,result)
    #----------------------------------------------------------------------
    def __logBetweenTwoColumns(self,data,col1,col2):
        return super().logBetweenTwoColumns(data,col1,col2)
    #----------------------------------------------------------------------
    def __logBetweenTwoColumnsWithBound(self,data,col1,col2,bound):
        result=data[[col1,col2]].copy()
        result['change']=self.__logBetweenTwoColumns(result,col1,col2)
        select=(result[col1]==0) & (result[col2]>0)
        result.loc[select,'change']=-bound
        select=(result[col1]>0) & (result[col2]==0)
        result.loc[select,'change']=bound
        return result['change']
    #----------------------------------------------------------------------
    def __buySellPressure(self,data,volumeStart,volumeEnd):
        result=data.copy()
        result['buyVolume']=0
        result['sellVolume']=0
        for i in range(volumeStart,volumeEnd+1):
            result['buyVolume']=result['buyVolume']+result['BV'+str(i)]
            result['sellVolume']=result['sellVolume']+result['SV'+str(i)]
            pass
        colname='buySellPressure'
        result[colname]=self.__logBetweenTwoColumns(result,'buyVolume','sellVolume')
        return result[colname]
        pass
    #----------------------------------------------------------------------
    def __averageVolumeWeighted(self,data,volumeStart,volumeEnd,decay):
        result=data.copy()
        result['buyAverageVolumeWeighted']=0
        result['sellAverageVolumeWeighted']=0
        for i in range(volumeStart,volumeEnd+1):
            result['buyAverageVolumeWeighted']=result['buyAverageVolumeWeighted']+result['BV'+str(i)]*math.pow(decay,i-volumeStart)
            result['sellAverageVolumeWeighted']=result['sellAverageVolumeWeighted']+result['SV'+str(i)]*math.pow(decay,i-volumeStart)
            pass
        result['buyAverageVolumeWeighted']=result['buyAverageVolumeWeighted']/(volumeEnd-volumeStart+1)
        result['sellAverageVolumeWeighted']=result['sellAverageVolumeWeighted']/(volumeEnd-volumeStart+1)
        return result[['buyAverageVolumeWeighted','sellAverageVolumeWeighted']]
        pass
    #----------------------------------------------------------------------
    def __buySellVolumeWeightedPressure(self,data,volumeStart,volumeEnd,decay):
        result=data.copy()
        result[['buyAverageVolumeWeighted','sellAverageVolumeWeighted']]=self.__averageVolumeWeighted(data,volumeStart,volumeEnd,decay)
        result['buySellVolumeWeightedPressure']=self.__logBetweenTwoColumns(result,'buyAverageVolumeWeighted','sellAverageVolumeWeighted')
        return result['buySellVolumeWeightedPressure']
        pass
    #----------------------------------------------------------------------
    def __buySellAmountWeightedPressure(self,data,amountStart,amountEnd,decay):
        result=data.copy()
        result[['buyAverageAmountWeighted','sellAverageAmountWeighted']]=self.__averageAmountWeighted(data,amountStart,amountEnd,decay)
        select=(result['buyAverageAmountWeighted']>0)&(result['sellAverageAmountWeighted']>0)
        result['buySellAmountWeightedPressure']=0
        result.loc[select,'buySellAmountWeightedPressure']=(result['buyAverageAmountWeighted']-result['sellAverageAmountWeighted'])/(result['buyAverageAmountWeighted']+result['sellAverageAmountWeighted'])
        return result['buySellAmountWeightedPressure']
        pass
    #----------------------------------------------------------------------
    def __averageAmountWeighted(self,data,amountStart,amountEnd,decay):
        result=data.copy()
        result['buyAverageAmountWeighted']=0
        result['sellAverageAmountWeighted']=0
        for i in range(amountStart,amountEnd+1):
            result['buyAverageAmountWeighted']=result['buyAverageAmountWeighted']+result['B'+str(i)]*result['BV'+str(i)]*math.pow(decay,i-amountStart)
            result['sellAverageAmountWeighted']=result['sellAverageAmountWeighted']+result['S'+str(i)]*result['SV'+str(i)]*math.pow(decay,i-amountStart)
            pass
        result['buyAverageAmountWeighted']=result['buyAverageAmountWeighted']/(amountEnd-amountStart+1)
        result['sellAverageAmountWeighted']=result['sellAverageAmountWeighted']/(amountEnd-amountStart+1)
        return result[['buyAverageAmountWeighted','sellAverageAmountWeighted']]
        pass
    #----------------------------------------------------------------------
    def __buySellWeightedVolumeRatio(self,data,n):
        data['buyWeightedVolume'+str(n)]=0
        data['sellWeightedVolume'+str(n)]=0
        select0=(data['B1']>0) & (data['S1']>0)
        for i in range(1,n+1):
            select=(data['B'+str(i)]>0) & select0
            data.loc[select,'buyWeightedVolume'+str(n)]=data['buyWeightedVolume'+str(n)]+data['BV'+str(i)]*(data['buySellSpread']*data['midPrice']/(data['midPrice']-data['B'+str(i)]))[select]
            select=(data['S'+str(i)]>0) & select0
            data.loc[select,'sellWeightedVolume'+str(n)]=data['sellWeightedVolume'+str(n)]+data['SV'+str(i)]*(data['buySellSpread']*data['midPrice']/(-data['midPrice']+data['S'+str(i)]))[select]
            pass
        data['buySellWeightedVolumeRatio'+str(n)]=data['buyWeightedVolume'+str(n)]/(data['buyWeightedVolume'+str(n)]+data['sellWeightedVolume'+str(n)])
        select=data['B1']==data['S1']
        data.loc[select,'buySellWeightedVolumeRatio'+str(n)]=(data['BV1']/(data['BV1']+data['SV1']))[select]
        select=(data['B1']==0) | (data['S1']==0)
        data.loc[select,'buySellWeightedVolumeRatio'+str(n)]=0
        pass
    #----------------------------------------------------------------------
    def __longTermVolumeIncreaeMean(self,data,code,date,span):
        result=data[['date','volumeIncrease']].copy()
        lastData=super().getLastTradedayTickData(code,date)
        if lastData.shape[0]>0:
            lastData=lastData[lastData['time']<'145700000']
            last=lastData[['date','volumeIncrease']].copy()
            total=pd.concat([last,result])
        else:
            total=result
        total['volumeIncreaseMean']=total['volumeIncrease'].rolling(span,min_periods=1).mean()
        select=total['date']==date
        return total[select]['volumeIncreaseMean']
        pass
    #----------------------------------------------------------------------
    def __buySellVolumeForce(self,data):
        mydata=data.copy()
        result=mydata.copy()
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
        return result[['buyForce','sellForce']]
        pass
    #----------------------------------------------------------------------
    def __buySellAmountForce(self,data):
        result=data.copy()
        select0=(result['S1']>result['B1']) & (result['B1']>0) & (result['S1'].shift(1)>result['B1'].shift(1)) & (result['B1'].shift(1)>0)#bid ask 价格不相同并且没有涨跌停
        result['buyForce']=0
        for i in range(1,11):
            select=(result['S1']>result['S'+str(i)].shift(1)) & select0
            result.loc[select,'buyForce']=(result['buyForce']+result['SV'+str(i)].shift(1)*result['S'+str(i)].shift(1)/(1+np.exp(-1000*(result['S'+str(i)].shift(1)/result['midPrice'].shift(1)-1))))[select]
            select=(result['S1']==result['S'+str(i)].shift(1)) & (result['SV'+str(i)].shift(1)>result['SV1']) & select0
            result.loc[select,'buyForce']=(result['buyForce']+(result['SV'+str(i)].shift(1)-result['SV1'])*result['S'+str(i)].shift(1)/(1+np.exp(-1000*(result['S'+str(i)].shift(1)/result['midPrice'].shift(1)-1))))[select]
            pass
        result['sellForce']=0
        for i in range(1,11):
            select=(result['B1']<result['B'+str(i)].shift(1)) & select0
            result.loc[select,'sellForce']=(result['sellForce']+result['BV'+str(i)].shift(1)*result['B'+str(i)].shift(1)/(1+np.exp(1000*(result['B'+str(i)].shift(1)/result['midPrice'].shift(1)-1))))[select]
            select=(result['B1']==result['B'+str(i)].shift(1)) & (result['BV'+str(i)].shift(1)>result['BV1']) & select0
            result.loc[select,'sellForce']=(result['sellForce']+(result['BV'+str(i)].shift(1)-result['BV1'])*result['B'+str(i)].shift(1)/(1+np.exp(1000*(result['B'+str(i)].shift(1)/result['midPrice'].shift(1)-1))))[select]
            pass
        return result[['buyForce','sellForce']]
        pass
    #----------------------------------------------------------------------
    def __volumeMagnification(self,data,fast,slow):
        result=data.copy()
        result['fast']=super().EMA(result['volumeIncrease'],fast)
        result['slow']=super().EMA(result['volumeIncrease'],slow)
        result['magnification']=0
        select=result['slow']>0
        result.loc[select,'magnification']=result['fast']/result['slow']
        return result['magnification']
        pass
    #----------------------------------------------------------------------
    def computerFactor(self,code,date,mydata):
        result=pd.DataFrame()
        if mydata.shape[0]!=0:
            #index对齐即可
            result=mydata.copy()
            #------------------------------------------------------------------
            #bid ask 间距，因子值在[0,0.1]之间
            result['buySellSpread']=0
            select=(mydata['S1']!=0) & (mydata['B1']!=0)
            result.loc[select,'buySellSpread']=((mydata['S1']-mydata['B1'])/mydata['midPrice'])[select]
            #------------------------------------------------------------------
            #计算成交量得MA
            result['volumeIncreaseMA3']=super().MA(result['volumeIncrease'],3)
            result['volumeIncreaseMA20']=super().MA(result['volumeIncrease'],20)
            result['volumeIncreaseMA40']=super().MA(result['volumeIncrease'],40)
            result['volumeIncreaseMA100']=super().MA(result['volumeIncrease'],100)
            result['volumeIncreaseMA200']=super().MA(result['volumeIncrease'],200)
            result['volumeIncreaseMean']=self.__longTermVolumeIncreaeMean(result,code,date,4741)
            select=result['volumeIncreaseMean']==0
            result['volumeIncreaseMA3ToMean']=result['volumeIncreaseMA3']/result['volumeIncreaseMean']
            result['volumeIncreaseMA20ToMean']=result['volumeIncreaseMA20']/result['volumeIncreaseMean']
            result['volumeIncreaseMA40ToMean']=result['volumeIncreaseMA40']/result['volumeIncreaseMean']
            result['volumeIncreaseMA100ToMean']=result['volumeIncreaseMA100']/result['volumeIncreaseMean']
            result['volumeIncreaseMA200ToMean']=result['volumeIncreaseMA200']/result['volumeIncreaseMean']
            result.loc[select,'volumeIncreaseMA3ToMean']=0
            result.loc[select,'volumeIncreaseMA20ToMean']=0
            result.loc[select,'volumeIncreaseMA40ToMean']=0
            result.loc[select,'volumeIncreaseMA100ToMean']=0
            result.loc[select,'volumeIncreaseMA200ToMean']=0
            result['volumeMagnification10_30']=self.__volumeMagnification(result,10,60)
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
            #挂单量信息
            select=(result['sellVolume10']+result['buyVolume10'])==0
            result['buySellVolumeRatio2']=(result['buyVolume2']/(result['sellVolume2']+result['buyVolume2']))
            result['buySellVolumeRatio5']=(result['buyVolume5']/(result['sellVolume5']+result['buyVolume5']))
            result['buySellVolumeRatio10']=(result['buyVolume10']/(result['sellVolume10']+result['buyVolume10']))
            result.loc[select,'buySellVolumeRatio2']=0
            result.loc[select,'buySellVolumeRatio5']=0
            result.loc[select,'buySellVolumeRatio10']=0
            #------------------------------------------------------------------
            #加权之后的多空力量对比
            #根据价格和量计算的多空力量对比因子值在[0,1]之间
            self.__buySellWeightedVolumeRatio(result,2)
            self.__buySellWeightedVolumeRatio(result,5)
            self.__buySellWeightedVolumeRatio(result,10)
            result[['buyAverageVolumeWeighted1_10_0.8','sellAverageVolumeWeighted1_10_0.8']]=self.__averageVolumeWeighted(result,1,10,0.8)
            result[['buyAverageVolume1_10','sellAverageVolume1_10']]=self.__averageVolumeWeighted(result,1,10,1)
            result[['buyAverageAmountWeighted1_10_0.8','sellAverageAmountWeighted1_10_0.8']]=self.__averageAmountWeighted(result,1,10,0.8)
            result['buySellVolumeWeightedPressure1_10_0.8']=self.__buySellVolumeWeightedPressure(result,1,10,0.8)
            result['EMABuySellVolumeWeightedPressure1_10_0.8']=super().EMA(result['buySellVolumeWeightedPressure1_10_0.8'],5)
            result['buySellAmountWeightedPressure1_10_0.8']=self.__buySellAmountWeightedPressure(result,1,10,0.8)
            result['EMABuySellAmountWeightedPressure1_10_0.8']=super().EMA(result['buySellAmountWeightedPressure1_10_0.8'],5)
            result['buySellPress1_10']=self.__buySellPressure(result,1,10)
            result['EMABuySellPress1_10']=super().EMA(result['buySellPress1_10'],10)

            #------------------------------------------------------------------
            #主动买和主动卖，因子值大小在[0,+∞)
            result[['buyForce','sellForce']]=self.__buySellAmountForce(result)
            result['EMABuyForce15']=super().EMA(result['buyForce'],15)
            result['EMASellForce15']=super().EMA(result['sellForce'],15)
            result['buySellForceChange']=self.__logBetweenTwoColumnsWithBound(result,'EMABuyForce15','EMASellForce15',10)
            result['volumeIncreaseMean']=self.__longTermVolumeIncreaeMean(result,code,date,4741)
            select=result['volumeIncreaseMean']==0
            result['buyForcePrice']=result['buyForce']/result['volumeIncreaseMean']
            result['sellForcePrice']=result['sellForce']/result['volumeIncreaseMean']
            result.loc[select,'buyForcePrice']=0
            result.loc[select,'sellForcePrice']=0

            #------------------------------------------------------------------
            #剔除14点57分之后，集合竞价的数据
            result=result[result['time']<'145700000']
            mycolumns=list(set(result.columns).difference(set(mydata.columns)))
            mycolumns.sort()
            result=result[mycolumns]
            super().checkDataNan(code,date,self.factor,result)
            pass
        else:
            logger.error(f'There no data of {code} in {date} to computer factor!') 
        return result
########################################################################
