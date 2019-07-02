import pandas as pd
from DataPrepare.tickFactors.statusOfTickShot.buySellForce import buySellForce
from DataPrepare.tickFactors.factorBase import factorBase

def buySellVolume(mydata:pd.DataFrame):
    #index对齐即可
    result=mydata.copy()
    forceBase=factorBase()
    force=buySellForce()
    #------------------------------------------------------------------
    #bid ask 间距，因子值在[0,0.1]之间
    result['buySellSpread']=0
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
    force._buySellForce__buySellWeightedVolumeRatio(result,2)
    force._buySellForce__buySellWeightedVolumeRatio(result,5)
    force._buySellForce__buySellWeightedVolumeRatio(result,10)
    result[['buyAverageVolumeWeighted1_10_0.8','sellAverageVolumeWeighted1_10_0.8']]=force._buySellForce__averageVolumeWeighted(result,1,10,0.8)
    result[['buyAverageVolume1_10','sellAverageVolume1_10']]=force._buySellForce__averageVolumeWeighted(result,1,10,1)
    result[['buyAverageAmountWeighted1_10_0.8','sellAverageAmountWeighted1_10_0.8']]=force._buySellForce__averageAmountWeighted(result,1,10,0.8)
    result['buySellVolumeWeightedPressure1_10_0.8']=force._buySellForce__buySellVolumeWeightedPressure(result,1,10,0.8)
    result['EMABuySellVolumeWeightedPressure1_10_0.8']=forceBase.EMA(result['buySellVolumeWeightedPressure1_10_0.8'],5)
    result['buySellAmountWeightedPressure1_10_0.8']=force._buySellForce__buySellAmountWeightedPressure(result,1,10,0.8)
    result['EMABuySellAmountWeightedPressure1_10_0.8']=forceBase.EMA(result['buySellAmountWeightedPressure1_10_0.8'],5)
    result['buySellPress1_10']=force._buySellForce__buySellPressure(result,1,10)
    result['EMABuySellPress1_10']=forceBase.EMA(result['buySellPress1_10'],10)

    #------------------------------------------------------------------
    #主动买和主动卖，因子值大小在[0,+∞)
    result[['buyForce','sellForce']]=force._buySellForce__buySellAmountForce(result)
    result['EMABuyForce15']=forceBase.EMA(result['buyForce'],15)
    result['EMASellForce15']=forceBase.EMA(result['sellForce'],15)
    result['buySellForceChange']=force._buySellForce__logBetweenTwoColumnsWithBound(result,'EMABuyForce15','EMASellForce15',10)
    #result['volumeIncreaseMean']=force._buySellForce__longTermVolumeIncreaeMean(result,code,date,4741)
    #select=result['volumeIncreaseMean']==0
    #result['buyForcePrice']=result['buyForce']/result['volumeIncreaseMean']
    #result['sellForcePrice']=result['sellForce']/result['volumeIncreaseMean']
    #result.loc[select,'buyForcePrice']=0
    #result.loc[select,'sellForcePrice']=0
    return result



