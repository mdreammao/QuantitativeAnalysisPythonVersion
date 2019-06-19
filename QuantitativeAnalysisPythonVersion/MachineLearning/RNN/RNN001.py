from MachineLearning.machineLeariningBase import machineLeariningBase
from DataAccess.TradedayDataProcess import TradedayDataProcess
import os
import pandas as pd
from Config.myConstant import *
from Config.myConfig import *
########################################################################
class RNN001(machineLeariningBase):
    """RNN算法"""
    #----------------------------------------------------------------------
    def __init__(self):
        pass
    #----------------------------------------------------------------------
    def myRNN(self,startDate,endDate,testStart,document):
        tradedays=TradedayDataProcess.getTradedays(startDate,endDate)
        filePath=os.path.join(LocalFileAddress,document)
        trainSet=[]
        testSet=[]
        for day in tradedays:
            fileName=os.path.join(filePath,str(day)+'.h5')
            data=super().getData(fileName)
            if day<str(testStart):
                trainSet.append(data)
            else:
                testSet.append(data)
            pass
        pass
        trainSet=pd.concat(trainSet)
        testSet=pd.concat(testSet)
        m=round(trainSet.corr(),3)
        print(m.loc[(m['midIncreaseNext1m'].abs()>=0.03),'midIncreaseNext1m'].sort_values())
        print(m.loc[(m['midIncreaseMinNext1m'].abs()>=0.03),'midIncreaseMinNext1m'].sort_values())
        print(m.loc[(m['midIncreaseMaxNext1m'].abs()>=0.03),'midIncreaseMaxNext1m'].sort_values())
        print(m.loc[(m['midIncreaseMinNext2m'].abs()>=0.03),'midIncreaseMinNext2m'].sort_values())
        print(m.loc[(m['midIncreaseMaxNext2m'].abs()>=0.03),'midIncreaseMaxNext2m'].sort_values())
        print(m.loc[(m['midIncreaseMinNext5m'].abs()>=0.03),'midIncreaseMinNext5m'].sort_values())
        print(m.loc[(m['midIncreaseMaxNext5m'].abs()>=0.03),'midIncreaseMaxNext5m'].sort_values())

########################################################################

