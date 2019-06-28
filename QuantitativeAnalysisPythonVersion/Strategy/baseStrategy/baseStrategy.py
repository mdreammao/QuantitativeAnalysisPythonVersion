from Config.myConstant import *
from Config.myConfig import *
from DataAccess.TradedayDataProcess import TradedayDataProcess
from DataPrepare.tickFactorsProcess import tickFactorsProcess
from Utility.JobLibUtility import JobLibUtility
import pandas as pd
import math
from typing import List as type_list

########################################################################
class baseStrategy(object):
    """策略回测的基类"""
    #----------------------------------------------------------------------
    def __init__(self):
        pass
    #----------------------------------------------------------------------
    def multipleCodes_parallel(self,codes:type_list[str],startDate:str,endDate:str,parameters:list):
        raise NotImplementedError()
    #----------------------------------------------------------------------
    def multipleCodes(self,codes:type_list[str],startDate:str,endDate:str,parameters:list):
        raise NotImplementedError()
    #----------------------------------------------------------------------
    def singleCode(self,code:str,startDate:str,endDate:str,parameters:list):
        raise NotImplementedError()
    #----------------------------------------------------------------------
    def strategy():
        raise NotImplementedError()
    #----------------------------------------------------------------------
    def buyByTickShotData(self,tick,myindex,targetPosition,priceDeviation=0):
        ceilPrice=tick[myindex['S1']]*(1+priceDeviation)
        if ceilPrice==0:
            ceilPrice=tick[myindex['B1']]*(1+priceDeviation)
        buyPosition=0
        buyAmount=0
        for i in range(1,11):
            price=tick[myindex['S'+str(i)]]
            volume=tick[myindex['SV'+str(i)]]
            if ((price<ceilPrice) & (targetPosition>0)):
                buyVolume=min(targetPosition,volume)
                buyPosition=buyPosition+buyVolume
                buyAmount=buyAmount+buyVolume*price
                targetPosition=targetPosition-buyVolume
                pass
            pass
        pass
        if buyPosition==0:
            return [ceilPrice,targetPosition]
        averagePrice=buyAmount/buyPosition
        buyPosition=math.floor(buyPosition*0.01)*100
        return [averagePrice,buyPosition]
    #----------------------------------------------------------------------
    def sellByTickShotData(self,tick,myindex,targetPosition,priceDeviation=0):
        floorPrice=tick[myindex['B1']]*(1-priceDeviation)
        if floorPrice==0:
            floorPrice=tick[myindex['S1']]*(1-priceDeviation)
        sellPosition=0
        sellAmount=0
        for i in range(1,11):
            price=tick[myindex['B'+str(i)]]
            volume=tick[myindex['BV'+str(i)]]
            if ((price>floorPrice) & (targetPosition>0)):
                sellVolume=min(targetPosition,volume)
                sellPosition=sellPosition+sellVolume
                sellAmount=sellAmount+sellVolume*price
                targetPosition=targetPosition-sellVolume
                pass
            pass
        pass
        if sellPosition==0:
            return [floorPrice,targetPosition]
        averagePrice=sellAmount/sellPosition
        sellPosition=math.floor(sellPosition*0.01)*100
        return [averagePrice,sellPosition]
########################################################################

