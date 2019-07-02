import unittest
import os
from Config.myConfig import *
from DataPrepare.tickFactors.statusOfTickShot.buySellForce import buySellForce
from Unitest.testFactor.mytry import myfun

class testBuySellForce(unittest.TestCase):
    """buySellForce因子的测试函数"""
    def test_buySellForce(self):
        for i in range(1000):
            fileName=os.path.join(LocalFileAddress,'test','buySellForce','case'+str(i)+'.h5')
            exists=os.path.isfile(fileName)
            myinstance=buySellForce()
            code='600000.SH'
            date=20181231
            if exists==True:
                with pd.HDFStore(inputFile,'r',complib='blosc:zstd',append=True,complevel=9) as store:
                    inputData=store['input']
                    outputData=store['ouput']
                    self.assertEqual(myinstance._buySellForce__computerFactor(code,date,inputData),outputData)
                pass
            pass
        pass
    pass

class testMyfun(unittest.TestCase):
    def test_myfun(self):
        self.assertEqual(myfun(2),4)
        pass
