from DataAccess.KLineDataProcess import *
from DataAccess.TradedayDataProcess import *
from DataAccess.IndexComponentDataProcess import *
from DataAccess.IndustryClassification import *
from DataAccess.StockIPOInfoProcess import *
from DataPrepare.dailyFactorsProcess import *
from DataAccess.IndexCode import *
from DataAccess.TickDataProcess import *
from Config.myConfig import *
from Config.myConfig import *
import datetime
import pandas as pd

########################################################################
class UpdateBasicData(object):
    """更新数据的辅助函数"""
    #----------------------------------------------------------------------
    @classmethod 
    def __init__(self):
        pass
    #----------------------------------------------------------------------
    @classmethod 
    def updateDailyAndMinuteAll(self):
        startDate=str(20100101)
        yesterday=(datetime.datetime.now()+datetime.timedelta(days=-1)).strftime("%Y%m%d")
        today=datetime.datetime.now().strftime("%Y%m%d")
        logger.info('update tradedays!')
        tradedays=TradedayDataProcess.getTradedays(startDate,yesterday)
        endDate=tradedays.max()
        logger.info('update index code list!')
        UpdateBasicData.updateIndexInfo(startDate,endDate)
        logger.info('update stock IPO Info!')
        UpdateBasicData.updateStockIPOInfo()
        logger.info('update stockCodes!')
        stockCodes=UpdateBasicData.updateStockCodes(startDate,endDate)
        logger.info('update stockList')
        #UpdateBasicData.updateStockList(startDate,endDate)
        logger.info('update stock daily KLines')
        #UpdateBasicData.updateMultipleStocksDailyKLines(stockCodes,startDate,endDate)
        logger.info('update stock daily derivative data')
        #UpdateBasicData.updateMultipleStocksDailyDerivatives(stockCodes,startDate,endDate)
        logger.info('update stock minute KLines')
        #UpdateBasicData.updateMultipleStocksMinuteKLines(stockCodes,startDate,endDate)
        logger.info('update index daily KLines')
        UpdateBasicData.updateDailyIndexKLines('000016.SH',startDate,endDate)
        UpdateBasicData.updateDailyIndexKLines('000300.SH',startDate,endDate)
        UpdateBasicData.updateDailyIndexKLines('000905.SH',startDate,endDate)
        logger.info('update index minute KLines')
        UpdateBasicData.updateMinuteIndexKLines('000016.SH',startDate,endDate)
        UpdateBasicData.updateMinuteIndexKLines('000300.SH',startDate,endDate)
        UpdateBasicData.updateMinuteIndexKLines('000905.SH',startDate,endDate)
        logger.info('update industry info')
        UpdateBasicData.updateIndustry()
        logger.info('update daily factors')
        factors=['closeStd','index','marketValue','industry']
        UpdateBasicData.updateDailyFactors(stockCodes,factors)
        pass
    #----------------------------------------------------------------------
    @classmethod 
    def updateTickAll(self):
        startDate=str(20180101)
        yesterday=(datetime.datetime.now()+datetime.timedelta(days=-1)).strftime("%Y%m%d")
        today=datetime.datetime.now().strftime("%Y%m%d")
        logger.info('update tradedays!')
        tradedays=TradedayDataProcess.getTradedays(startDate,yesterday)
        endDate=tradedays.max()
        logger.info('update index code list!')
        UpdateBasicData.updateIndexInfo(startDate,endDate)
        logger.info('update stock IPO Info!')
        UpdateBasicData.updateStockIPOInfo()
        logger.info('update stockCodes!')
        stockCodes=UpdateBasicData.updateStockCodes(startDate,endDate)
        logger.info('update stockList')
        UpdateBasicData.updateStockList(startDate,endDate)
        logger.info('update tickShots')
        UpdateBasicData.updateMultipleStocksTickShots(stockCodes,startDate,endDate)
    #----------------------------------------------------------------------
    @classmethod 
    def updateStockIPOInfo(self):
        StockIPOInfoProcess.updateIPOInfoFromLocalFile()
        pass
    #----------------------------------------------------------------------
    @classmethod 
    def updateStockList(self,startDate,endDate):
        StockIPOInfoProcess.updateStockListFromLocalFile(startDate,endDate)
        pass
    #----------------------------------------------------------------------
    @classmethod 
    def updateStockCodes(self,startDate,endDate):
        localFileStr=os.path.join(LocalFileAddress,'stockCode.h5')
        exists=HDF5Utility.fileCheck(localFileStr)
        if exists==True:
            store=pd.HDFStore(localFileStr,'a',complib='blosc:zstd',append=False,complevel=9)
            lastDate=store['date'].iloc[-1]['date']
            if lastDate<endDate:
                exists=False
            store.close()
            pass
        if exists==True:
            store=pd.HDFStore(localFileStr,'a',complib='blosc:zstd',append=False,complevel=9)
            stockCodes=store['data']
            store.close()
            pass
        else:
            myindex=IndexComponentDataProcess()
            index500=myindex.getCSI500DataByDate(startDate,endDate)
            index300=myindex.getHS300DataByDate(startDate,endDate)
            index50=myindex.getSSE50DataByDate(startDate,endDate)
            stockCodes=(pd.concat([index500,index300,index50],ignore_index=True)['code'].drop_duplicates())
            stockCodes=stockCodes.sort_values()
            days=pd.DataFrame(TradedayDataProcess.getTradedays(startDate,endDate))
            store=pd.HDFStore(localFileStr,'a',complib='blosc:zstd',append=False,complevel=9)
            store['data']=stockCodes
            store['date']=days
            store.close()
            pass
        return list(stockCodes)
        pass
    #----------------------------------------------------------------------
    @classmethod 
    def updateIndustry(self):
        IndustryClassification.updateIndustryInfo()
        pass
    #----------------------------------------------------------------------
    @classmethod 
    def updateIndexInfo(self,startDate,endDate):
        IndexCode.updateIndexCodeFromLocalFile()
        myindex=IndexComponentDataProcess(True)
        myindex.updateIndexComponentFromLocalFile(HS300,startDate,endDate)
        myindex.updateIndexComponentFromLocalFile(SSE50,startDate,endDate)
        myindex.updateIndexComponentFromLocalFile(CSI500,startDate,endDate)
        myindex.updateIndexEntryAndRemoveFromLocalFile(HS300)
        myindex.updateIndexEntryAndRemoveFromLocalFile(SSE50)
        myindex.updateIndexEntryAndRemoveFromLocalFile(CSI500)
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
    def updateMultipleStocksTickShots(self,codeList,startDate,endDate):
        tickStock=TickDataProcess()
        tickStock.parallelizationUpdateDataByDate(codeList,startDate,endDate)
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