from Config.myConstant import *
from Config.myConfig import *
from DataAccess.IndexComponentDataProcess import *
from DataAccess.KLineDataProcess import *
from DataAccess.TradedayDataProcess import *
from DataAccess.IndustryClassification import *
from DataAccess.IndexCode import *
from DataAccess.StockSharesProcess import *
from DataPrepare.dataPrepareByIndex import *
from DataPrepare.dailyKLineDataPrepared import *
from Strategy.stockReverseMovement import *
from Strategy.myRandomForestForCeiling import *
from Strategy.stockReverseByStd import *
from Utility.mytest import *
import warnings
import time


#----------------------------------------------------------------------
def main():
    """主程序入口"""
    warnings.filterwarnings('ignore')
    startDate=20100101
    endDate=20190415
    fileStr=LocalFileAddress+"\\{0}\\{1}.h5".format('dailyFactors','stockCodes')
    store = pd.HDFStore(fileStr,'a')
    stockCodes=list(store.select('stockCodes')['code'])
    store.close()
    #stockCodes=list(['601398.SH'])
    temp=stockReverseByStd()
    #temp.reverse(stockCodes,startDate,endDate)
    temp.reverseByJit(stockCodes,startDate,endDate)
    pass
if __name__ == '__main__':
    main()
