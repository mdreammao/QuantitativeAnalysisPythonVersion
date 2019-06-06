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
from DataPrepare.dailyFactorsProcess import dailyFactorsProcess
from DataAccess.TickDataProcess import *
from DataPrepare.tickFactors.factorBase import factorBase
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
    def getTickDataAndFactorsByDateFromLocalFile(self,code,date,factors=TICKFACTORSUSED):
        myfactor=factorBase()
        mydata=pd.DataFrame()
        for item in factors:
            factor=item['factor']
            data=myfactor.getDataFromLocalFile(code,date,factor)
            if mydata.shape[0]==0: #如果还没有取出来数据
                mydata=data.copy()
                pass
            elif data.shape[0]!=0:
                mydata=pd.merge(mydata,data,how='left',left_index=True,right_index=True)
                pass
        tick=TickDataProcess()
        tickData=tick.getDataByDateFromLocalFile(code,date)
        mydata=pd.merge(mydata,tickData,how='left',left_index=True,right_index=True)
        if mydata.shape[0]==0:
            return mydata
        dailyFactor=['closeStd','index','marketValue','industry']
        dailyRepo=dailyFactorsProcess()
        dailyData=dailyRepo.getSingleStockDailyFactors(code,dailyFactor,date,date)
        for col in dailyData.columns:
            if col not in ['date','code','return']:
                mydata[col]=dailyData[col].iloc[0]
        dailyKLineRepo=KLineDataProcess('daily')
        dailyKLineData=dailyKLineRepo.getDataByDate(code,date,date)
        mydata['preClose']=dailyKLineData['preClose'].iloc[0]
        mydata['increaseToday']=mydata['midPrice']/mydata['preClose']-1
        return mydata
    #----------------------------------------------------------------------
    def getFactorsUsedByDateFromLocalFile(self,code,date,factors=TICKFACTORSUSED):
        myfactor=factorBase()
        mydata=pd.DataFrame()
        for item in factors:
            factor=item['factor']
            data=myfactor.getDataFromLocalFile(code,date,factor)
            if mydata.shape[0]==0: #如果还没有取出来数据
                mydata=data.copy()
                pass
            elif data.shape[0]!=0:
                mydata=pd.merge(mydata,data,how='left',left_index=True,right_index=True)
                pass
        return mydata
    #----------------------------------------------------------------------
    def getDataByDateFromLocalFile(self,code,date,factor):
        myfactor=factorBase()
        mydata=myfactor.getDataFromLocalFile(code,date,factor)
        return mydata
    #----------------------------------------------------------------------
    def updateAllFactorsByCodeAndDate(self,code,date):
        code=str(code)
        date=str(date)
        data=pd.DataFrame()
        logger.info(f'Compute factors of {code} in {date} start!')
        factorList=TICKFACTORSNEEDTOUPDATE
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
    #输入日期和股票列表，获取当日全部股票列表的因子
    def getLotsDataByDate(self,StockCodes,date,factors=TICKFACTORSUSED):
        all=[]
        for i in range(len(StockCodes)):
            code=StockCodes[i]
            mydata=self.getTickDataAndFactorsByDateFromLocalFile(code,date,factors)
            all.append(mydata)
        all=pd.concat(all)
        return all
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
    #----------------------------------------------------------------------
    def parallelizationGetDataByDate(self,stockCodes,date,factors=TICKFACTORSUSED):
        data=JobLibUtility.useJobLibToGetFactorDataDaily(self.getLotsDataByDate,stockCodes,MYGROUPS,date,factors)
        return data
        pass
########################################################################