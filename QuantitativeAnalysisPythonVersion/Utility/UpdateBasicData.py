from DataAccess.KLineDataProcess import *
from DataAccess.TradedayDataProcess import *
from DataAccess.IndexComponentDataProcess import *
from DataAccess.IndustryClassification import *
from DataPrepare.dailyFactorsProcess import *
from Config.myConfig import *
import datetime
import pandas as pd

########################################################################
class UpdateBasicData(object):
    """更新数据的辅助函数"""
    #----------------------------------------------------------------------
    def __init__(self):
        pass
    #----------------------------------------------------------------------
    @classmethod 
    def updateAll():
        startDate=str(20100101)
        today=datetime.datetime.now().strftime("%Y%m%d")
        print('update tradedays!')
        tradedays=TradedayDataProcess.getTradedays(startDate,today)
        endDate=tradedays.max()
        print('update codeList!')
        stockCodes=UpdateBasicData.updateCodeList(startDate,endDate)
        print('update stock daily KLines')
        UpdateBasicData.updateMultipleStocksDailyKLines(stockCodes,startDate,endDate)
        print('update stock daily derivative data')
        UpdateBasicData.updateMultipleStocksDailyDerivatives(stockCodes,startDate,endDate)
        print('update stock minute KLines')
        UpdateBasicData.updateMultipleStocksMinuteKLines(stockCodes,startDate,endDate)
        print('update index daily KLines')
        UpdateBasicData.updateDailyIndexKLines('000016.SH',startDate,endDate)
        UpdateBasicData.updateDailyIndexKLines('000300.SH',startDate,endDate)
        UpdateBasicData.updateDailyIndexKLines('000905.SH',startDate,endDate)
        print('update index minute KLines')
        UpdateBasicData.updateMinuteIndexKLines('000016.SH',startDate,endDate)
        UpdateBasicData.updateMinuteIndexKLines('000300.SH',startDate,endDate)
        UpdateBasicData.updateMinuteIndexKLines('000905.SH',startDate,endDate)
        print('update industry info')
        UpdateBasicData.updateIndustry()
        print('update daily factors')
        factors=['closeStd','index','marketValue','industry']
        UpdateBasicData.updateDailyFactors(stockCodes,factors)
        pass
    #----------------------------------------------------------------------
    @classmethod 
    def updateCodeList(self,startDate,endDate):
        myindex=IndexComponentDataProcess(True)
        index500=myindex.getCSI500DataByDate(startDate,endDate)
        index300=myindex.getHS300DataByDate(startDate,endDate)
        index50=myindex.getSSE50DataByDate(startDate,endDate)
        stockCodes=list(pd.concat([index500,index300,index50],ignore_index=True)['code'].drop_duplicates())
        HDF5Utility.pathCreate(LocalFileAddress)
        localFileStr=os.path.join(LocalFileAddress,'stockCode.h5')
        store=pd.HDFStore(localFileStr,'a',complib='blosc:zstd',append=False,complevel=9)
        store['data']=stockCodes
        store.close()
        return stockCodes
        pass
    #----------------------------------------------------------------------
    @classmethod 
    def updateIndustry():
        IndustryClassification.updateIndustryInfo()
        pass
    #----------------------------------------------------------------------
    @classmethod 
    def updateIndexInfo(self,startDate,endDate):
        
        pass
    #----------------------------------------------------------------------
    @classmethod 
    def updateSingleStockDailyKLines(self,code,startDate,endDate):
        dailyStock=KLineDataProcess('daily',True)
        dailyStock.getDataByDate(code,startDate,endDate)
        pass
    #----------------------------------------------------------------------
    @classmethod 
    def updateMultipleStocksDailyKLines(self,codeList,startDate,endDate):
        stock=KLineDataProcess('daily',True)
        stock.parallelizationUpdateDataByDate(codeList,startDate,endDate)
        pass
    #----------------------------------------------------------------------
    @classmethod 
    def updateSingleStockMinuteKLines(self,code,startDate,endDate):
        stock=KLineDataProcess('minute',True)
        stock.getDataByDate(code,startDate,endDate)
        pass
    #----------------------------------------------------------------------
    @classmethod 
    def updateMultipleStocksMinuteKLines(self,codeList,startDate,endDate):
        stock=KLineDataProcess('minute',True)
        stock.parallelizationUpdateDataByDate(codeList,startDate,endDate)
        pass
    #----------------------------------------------------------------------
    @classmethod 
    def updateSingleStockDailyDerivatives(self,code,startDate,endDate):
        stock=KLineDataProcess('dailyDerivative',True)
        stock.getDataByDate(code,startDate,endDate)
        pass
    #----------------------------------------------------------------------
    @classmethod 
    def updateMultipleStocksDailyDerivatives(self,codeList,startDate,endDate):
        dailyStock=KLineDataProcess('dailyDerivative',True)
        dailyStock.parallelizationUpdateDataByDate(codeList,startDate,endDate)
        pass
    #----------------------------------------------------------------------
    @classmethod 
    def updateDailyFactors(self,codeList,factors):
        dailyFactor=dailyFactorsProcess()
        dailyFactor.parallelizationUpdateFactors(codeList,factors)
        pass
    #----------------------------------------------------------------------
    @classmethod 
    def updateDailyIndexKLines(self,code,startDate,endDate):
        index=KLineDataProcess('dailyIndex',True)
        index.getDataByDate(code,startDate,endDate)
        pass
    #----------------------------------------------------------------------
    @classmethod 
    def updateMinuteIndexKLines(self,code,startDate,endDate):
        index=KLineDataProcess('minuteIndex',True)
        index.getDataByDate(code,startDate,endDate)
        pass
    #----------------------------------------------------------------------
    @classmethod 
    def updateMinuteFactors():
        pass
    #----------------------------------------------------------------------
    @classmethod 
    def updateTickShots():
        pass
    #----------------------------------------------------------------------
    @classmethod 
    def updateTickFactors():
        pass
########################################################################