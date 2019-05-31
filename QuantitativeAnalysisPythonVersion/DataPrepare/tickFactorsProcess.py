from Config.myConstant import *
from Config.myConfig import *
from Utility.ComputeUtility import *
from Utility.HDF5Utility import *
from Utility.JobLibUtility import *
from DataAccess.KLineDataProcess import *
from DataAccess.TradedayDataProcess import *
from DataAccess.StockSharesProcess import *
from DataAccess.StockIPOInfoProcess import *
from DataAccess.IndustryClassification import *
from DataAccess.IndexComponentDataProcess import *
from DataAccess.TickDataProcess import *
import importlib
import numpy as np
import datetime 
########################################################################
class tickFactorsProcess(object):
    """计算tick因子"""
    #----------------------------------------------------------------------
    def __init__(self):
        pass
    #----------------------------------------------------------------------
    def updateAllFactorsByCodeAndDays(self,code,startDate,endDate):
        tradedays=TradedayDataProcess.getTradedays(startDate,endDate)
        for date in tradedays:
            self.updateAllFactorsByCodeAndDate(code,date)
        pass
    
    #----------------------------------------------------------------------
    def updateAllFactorsByCodeAndDate(self,code,date):
        code=str(code)
        date=str(date)
        data=pd.DataFrame()
        logger.info(f'Compute factors of {code} in {date} start!')
        factorList=tickFactorsNeedToUpdate
        for factor in factorList:
            mymodule = importlib.import_module(factor['module'])
            myclass=getattr(mymodule, factor['class'])
            myinstance=myclass()
            exists=myinstance.checkLocalFile(code,date,factor['factor'])
            if exists==False:
                if data.shape[0]==0:
                    tick=TickDataProcess()
                    data=tick.getDataByDateFromLocalFile(code,date)
                    if data.shape[0]==0:
                        logger.warning(f'There is no tickShots of {code} in {date}')
                        return
                    pass
                myinstance.updateFactor(code,date,data)
                pass
            
    #----------------------------------------------------------------------
    #输入code=600000.SH，startdate=yyyyMMdd，endDate=yyyyMMdd
    def updateLotsDataByDate(self,StockCodes,startDate,endDate):
        for i in range(len(StockCodes)):
            code=StockCodes[i]
            self.updateAllFactorsByCodeAndDays(code,str(startDate),str(endDate))
    #----------------------------------------------------------------------
    def parallelizationUpdateDataByDate(self,stockCodes,startDate,endDate):
        JobLibUtility.useJobLibToUpdateData(self.updateLotsDataByDate,stockCodes,MYGROUPS,startDate,endDate)
        pass
########################################################################