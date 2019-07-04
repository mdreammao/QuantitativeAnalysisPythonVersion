import unittest
import os
import numpy as np
from Config.myConfig import *
from DataPrepare.tickFactors.statusOfTickShot.buySellForce import buySellForce
from DataPrepare.tickFactors.factorBase import factorBase
from Unitest.testFactor.tickFactorFunction import *

########################################################################
class testBuySellForce(unittest.TestCase):
    """buySellForce因子的测试函数"""
    #------------------------------------------------------------------
    def test_volume(self):
        for i in range(1000):
            inputFile=os.path.join(LocalFileAddress,'test','volume','input'+str(i)+'.csv')
            outputFile=os.path.join(LocalFileAddress,'test','volume','output'+str(i)+'.csv')
            exists=os.path.isfile(inputFile)
            if exists==True:
                input=pd.read_csv(inputFile,encoding='gbk')  
                outputTrue=pd.read_csv(outputFile,encoding='gbk')
                myoutput=buySellVolume(input)
                columns=list(outputTrue.columns)
                print(columns)
                for col in columns:
                    self.assertEqual(list(np.round(myoutput[col],8)),list(np.round(outputTrue[col],8)))
                pass
            pass
        pass
    pass
    #------------------------------------------------------------------
    def test_MA(self):
        ma=factorBase.MA
        for i in range(1000):
            inputFile=os.path.join(LocalFileAddress,'test','MA','input'+str(i)+'.csv')
            outputFile=os.path.join(LocalFileAddress,'test','MA','output'+str(i)+'.csv')
            exists=os.path.isfile(inputFile)
            if exists==True:
                input=pd.read_csv(inputFile,encoding='gbk')  
                outputTrue=pd.read_csv(outputFile,encoding='gbk')
                columns=list(input.columns)
                for col in columns:
                    myoutput=ma(input[col],10)
                    self.assertEqual(list(np.round(myoutput)),list(np.round(outputTrue[col],8)))
                pass
            pass
    #------------------------------------------------------------------
    def test_EMA(self):
        ema=factorBase.EMA
        for i in range(1000):
            inputFile=os.path.join(LocalFileAddress,'test','EMA','input'+str(i)+'.csv')
            outputFile=os.path.join(LocalFileAddress,'test','EMA','output'+str(i)+'.csv')
            exists=os.path.isfile(inputFile)
            if exists==True:
                input=pd.read_csv(inputFile,encoding='gbk')  
                outputTrue=pd.read_csv(outputFile,encoding='gbk')
                columns=list(input.columns)
                for col in columns:
                    myoutput=ema(input[col],10)
                    self.assertEqual(list(np.round(myoutput)),list(np.round(outputTrue[col],8)))
                pass
            pass
    #------------------------------------------------------------------
    def test_log(self):
        log=factorBase.logBetweenTwoColumns
        for i in range(1000):
            inputFile=os.path.join(LocalFileAddress,'test','LOG','input'+str(i)+'.csv')
            outputFile=os.path.join(LocalFileAddress,'test','LOG','output'+str(i)+'.csv')
            exists=os.path.isfile(inputFile)
            if exists==True:
                input=pd.read_csv(inputFile,encoding='gbk')  
                outputTrue=pd.read_csv(outputFile,encoding='gbk')
                myoutput=log(input,'col1','col2')
                self.assertEqual(list(np.round(myoutput)),list(np.round(outputTrue,8)))
                pass
            pass
########################################################################  



