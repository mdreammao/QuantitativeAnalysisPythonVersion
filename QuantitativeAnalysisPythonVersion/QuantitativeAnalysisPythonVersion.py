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
    '''
    mytest0=mytest()
    mytest0.testnumba()
    '''
    startDate=20100101
    endDate=20190415
    fileStr=LocalFileAddress+"\\{0}\\{1}.h5".format('dailyFactors','stockCodes')
    store = pd.HDFStore(fileStr,'a')
    stockCodes=list(store.select('stockCodes')['code'])
    store.close()
    #myindex=IndexComponentDataProcess()
    #index50=myindex.getSSE50DataByDate(endDate,endDate)
    #stockCodes=list(index50['code'].drop_duplicates())


    #stockCodes=list({'601299.SH'})
    #IndustryClassification.getIndustryByCode('600000.SH',20070101,20180410)
    #tmp=IndexComponentDataProcess()
    #tmp.getStockBelongs('600000.SH','000300.SH',20070101,20180410)
    #tmp=dailyKLineDataPrepared()
    #tmp.getStockDailyFeatureData(20070101,20180410)
    #StockSharesProcess.getStockShares('600000.SH',20170101,20190410)
    #IndexCode.getIndexCodeInfo()
    #IndustryClassification.getIndustryByCode('600000.SH',20190409)
    #tmp=stockReverseMovement()
    #tmp.reverse(20100101,20181228)
    #tmp=myRandomForestForCeiling()
    #tmp.myRandomForest('ceilingInNext5m')
    #tmp=dailyKLineDataPrepared()
    #tmp.getStockDailyFeatureData(stockCodes,startDate,endDate)
    temp=stockReverseByStd()
    temp.reverse(stockCodes,startDate,endDate)
    pass
if __name__ == '__main__':
    main()
