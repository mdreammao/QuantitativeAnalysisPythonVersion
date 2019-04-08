from Config.myConstant import *
from Config.myConfig import *
from DataAccess.IndexComponentDataProcess import *
from DataAccess.KLineDataProcess import *
from DataAccess.TradedayDataProcess import *
from DataPrepare.dataPrepareByIndex import *
from Strategy.stockReverseMovement import *
from Strategy.myRandomForestForCeiling import *
import time

#----------------------------------------------------------------------
def main():
    """主程序入口"""
    tmp=stockReverseMovement()
    tmp.reverse(20100101,20181228)
    #tmp=myRandomForestForCeiling()
    #tmp.myRandomForest('ceilingInNext10m')
    
if __name__ == '__main__':
    main()
