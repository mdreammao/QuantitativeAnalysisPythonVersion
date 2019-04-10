from Config.myConstant import *
from Config.myConfig import *
from DataAccess.IndexComponentDataProcess import *
from DataAccess.KLineDataProcess import *
from DataAccess.TradedayDataProcess import *
from DataAccess.IndustryClassification import *
from DataAccess.IndexCode import *
from DataAccess.StockSharesProcess import *
from DataPrepare.dataPrepareByIndex import *
from Strategy.stockReverseMovement import *
from Strategy.myRandomForestForCeiling import *
import time

#----------------------------------------------------------------------
def main():
    """主程序入口"""
    StockSharesProcess.getStockShares('600000.SH',20170101,20190410)
    #IndexCode.getIndexCodeInfo()
    #IndustryClassification.getIndustryByCode('600000.SH',20190409)
    #tmp=stockReverseMovement()
    #tmp.reverse(20100101,20181228)
    #tmp=myRandomForestForCeiling()
    #tmp.myRandomForest('ceilingInNext5m')
    
if __name__ == '__main__':
    main()
