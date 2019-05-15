import numpy as np
import pandas as pd
import math

########################################################################
class ComputeUtility(object):
    """数学计算的工具函数"""
    #----------------------------------------------------------------------
    def __init__(self):
        pass
    #----------------------------------------------------------------------
    """计算时间序列上的分位数"""
    @classmethod 
    def computeTimeSeriesRank(self,mydata,parameters):
        if len(parameters)==4:
            col1=parameters[0]
            col2=parameters[1]
            period=parameters[2]
            ratio=parameters[3]
        else:
            col1='closeStd20'
            col2='ts_rank_closeStd20'
            period=50
            ratio=0.4
        myMinPeriods=math.floor(period*ratio)
        mydata[col2]=mydata[col1].rolling(period,min_periods=myMinPeriods).apply((lambda x:pd.Series(x).rank().iloc[-1]/len(x)),raw=True)
        return mydata
        pass
    #----------------------------------------------------------------------
    """计算传入数据的标准差"""
    @classmethod 
    def computeStandardDeviation(self,mydata,parameters):
        if len(parameters)==4:
            col1=parameters[0]
            col2=parameters[1]
            period=parameters[2]
            ratio=parameters[3]
        else:
            col1='return'
            col2='closeStd20'
            period=20
            ratio=0.8
        myMinPeriods=math.floor(period*ratio)
        mydata[col2]=mydata[col1].shift(1).rolling(period,min_periods=myMinPeriods).std()
        return mydata
        pass
    #----------------------------------------------------------------------
    """计算传入数据两列之间的变化率"""
    @classmethod 
    def computeReturn(self,mydata,parameters):
        if len(parameters)==3:
            col1=parameters[0]
            col2=parameters[1]
            col3=parameters[2]
        else:
            col1='close'
            col2='preClose'
            col3='return'
        mydata[col3]=(mydata[col1]-mydata[col2])/mydata[col2]
        return mydata
        pass
########################################################################

