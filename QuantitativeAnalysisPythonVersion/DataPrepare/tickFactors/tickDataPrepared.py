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
from DataAccess.TickDataProcess import TickDataProcess
from DataPrepare.tickFactors.factorBase import factorBase
from Utility.InfluxdbUtility import InfluxdbUtility
import importlib
import numpy as np
import datetime 
########################################################################
class tickDataPrepared(object):
    """计算tick因子"""
    #----------------------------------------------------------------------
    def __init__(self,path='temp',factorList=TICKFACTORSUSED):
        self.path=os.path.join(LocalFileAddress,path)
        self.factorsUsed=factorList
        pass
    #----------------------------------------------------------------------
    def saveAllFactorsByCodeAndDays(self,code,startDate,endDate):
        tradedays=TradedayDataProcess.getTradedays(startDate,endDate)
        for date in tradedays:
            self.saveAllFactorsByCodeAndDate(code,date)
        pass
    #----------------------------------------------------------------------
    def saveAllFactorsToInfluxdbByCodeAndDays(self,code,startDate,endDate):
        tradedays=TradedayDataProcess.getTradedays(startDate,endDate)
        for date in tradedays:
            self.saveAllFactorsToInfluxdbByCodeAndDay(code,date)
        pass
    #----------------------------------------------------------------------
    def updateAllFactorsByCodeAndDays(self,code,startDate,endDate):
        tradedays=TradedayDataProcess.getTradedays(startDate,endDate)
        for date in tradedays:
            self.updateAllFactorsByCodeAndDate(code,date)
        pass
    #----------------------------------------------------------------------
    def getTickFactorsOnlyByDateFromLocalFile(self,code,date,factors=TICKFACTORSUSED):
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
        #dailyFactor=['closeStd','index','marketValue','industry']
        dailyRepo=dailyFactorsProcess()
        dailyData=dailyRepo.getSingleStockDailyFactors(code,date,date)
        for col in dailyData.columns:
            if col not in ['date','code','return']:
                mydata[col]=dailyData[col].iloc[0]
        dailyKLineRepo=KLineDataProcess('daily')
        dailyKLineData=dailyKLineRepo.getDataByDate(code,date,date)
        mydata['preClose']=dailyKLineData['preClose'].iloc[0]
        mydata['increaseToday']=mydata['midPrice']/mydata['preClose']-1
        ceiling=mydata[(mydata['B1']==0) | (mydata['S1']==0)]
        if ceiling.shape[0]>0:
            ceilingTime=ceiling['time'].iloc[0]
            mydata=mydata[mydata['time']<ceilingTime]
            pass
        
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
    def saveAllFactorsToInfluxdbByCodeAndDay(self,code,date):
        code=str(code)
        date=str(date)
        database=INFLUXDBTICKFACTORDATABASE
        measurement=str(code)
        tag={}
        myfactor=factorBase()
        mydata=pd.DataFrame()
        data=pd.DataFrame()
        factorList=TICKFACTORSNEEDTOUPDATE
        for factor in factorList:
            mymodule = importlib.import_module(factor['module'])
            myclass=getattr(mymodule, factor['class'])
            myinstance=myclass()
            if data.shape[0]==0:
                tick=TickDataProcess()
                data=tick.getDataByDateFromLocalFile(code,date)
                if data.shape[0]==0:
                    #logger.warning(f'There is no tickShots of {code} in {date}')
                    return
                highLimit=data.iloc[0]['highLimit']
                preClose=data.iloc[0]['dailyPreClose']
                if (highLimit/preClose-1)<0.06:
                    #logger.warning(f'The stock {code} is ST in {date}')
                    return
                pass
            factorData=myinstance.computerFactor(code,date,data)
            if factorData.shape[0]>0:
                if mydata.shape[0]==0:
                    mydata=factorData
                else:
                    mydata=pd.merge(mydata,factorData,how='left',left_index=True,right_index=True)
        #合并tick行情数据
        mydata=pd.merge(mydata,data[['code','date','time','midPrice','realData','dailyPreClose','dailyOpen','B1','S1','BV1','SV1']],how='left',left_index=True,right_index=True)
        if mydata.shape[0]==0:
            return 
        mydata['increaseToday']=mydata['midPrice']/mydata['dailyPreClose']-1
        mydata=mydata[mydata['time']<'145700000']
        #删去涨跌停之后的数据
        ceiling=mydata[(mydata['B1']==0) | (mydata['S1']==0)]
        if ceiling.shape[0]>0:
            ceilingTime=ceiling['time'].iloc[0]
            mydata=mydata[mydata['time']<ceilingTime]
            pass
        if mydata.shape[0]==0:
            return
        try:
            logger.info(f'Recording factors to influxdb of {code} in {date}!')
            InfluxdbUtility.saveDataFrameDataToInfluxdb(mydata,database,measurement,tag)
        except Exception as excp:
            logger.error(f'{fileName} error! {excp}')
        pass
    #----------------------------------------------------------------------
    def saveAllFactorsByCodeAndDate(self,code,date):
        mypath=os.path.join(self.path,str(code).replace('.','_'))
        HDF5Utility.pathCreate(mypath)
        fileName=os.path.join(mypath,str(date)+'.h5')
        exists=HDF5Utility.fileCheck(fileName)
        if exists==True:#如果文件已将存在，直接返回
            return 
        myfactor=factorBase()
        mydata=pd.DataFrame()
        factors=self.factorsUsed
        #获取tick因子数据
        mydata=self.getFactorsUsedByDateFromLocalFile(code,date,factors)
        
        #获取tick行情数据
        tick=TickDataProcess()
        tickData=tick.getDataByDateFromLocalFile(code,date)
        mydata=pd.merge(mydata,tickData,how='left',left_index=True,right_index=True)
        if mydata.shape[0]==0:
            return 
        #获取日线数据
        dailyRepo=dailyFactorsProcess()
        dailyData=dailyRepo.getSingleStockDailyFactors(code,date,date)
        dailyKLineRepo=KLineDataProcess('daily')
        dailyKLineData=dailyKLineRepo.getDataByDate(code,date,date)
        mydata['preClose']=dailyKLineData['preClose'].iloc[0]
        mydata['increaseToday']=mydata['midPrice']/mydata['preClose']-1
        mydata=mydata[mydata['time']<'145700000']
        #删去涨跌停之后的数据
        ceiling=mydata[(mydata['B1']==0) | (mydata['S1']==0)]
        if ceiling.shape[0]>0:
            ceilingTime=ceiling['time'].iloc[0]
            mydata=mydata[mydata['time']<ceilingTime]
            pass
        excludedColumns=['preClose','buyVolume2','buyVolume5','buyVolume10','sellVolume2','sellVolume5','sellVolume10']
        mycolumns=list(set(mydata.columns).difference(set(list(tickData.columns)+excludedColumns)))
        mycolumns.sort()
        mydata=mydata[mycolumns]
        if mydata.shape[0]==0:
            return
        try:
            logger.info(f'Recording factors of {code} in {date}!')
            with pd.HDFStore(fileName,'a',complib='blosc:zstd',append=True,complevel=9) as store:
                store.append('data',mydata,append=True,format="table",data_columns=mydata.columns)
        except Exception as excp:
            logger.error(f'{fileName} error! {excp}')
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
                #myinstance.updateFactorToInfluxdb(code,date,data)
                pass
            
    #----------------------------------------------------------------------
    def saveDataByCodeList(self,codeList,startDate,endDate):
        tradedays=TradedayDataProcess.getTradedays(startDate,endDate)
        for day in tradedays:
            data=self.parallelizationGetDataByDate(codeList,day)
            data=data[(data['time']>='093000000') & (data['time']<'145700000')]
            tickColumns=[ 'code', 'date', 'time', 'lastPrice', 'S1', 'S2','S3', 'S4', 'S5', 'S6', 'S7','S8', 'S9', 'S10', 'B1', 'B2', 'B3', 'B4','B5', 'B6', 'B7', 'B8', 'B9', 'B10', 'SV1', 'SV2', 'SV3', 'SV4', 'SV5','SV6', 'SV7', 'SV8', 'SV9', 'SV10', 'BV1', 'BV2', 'BV3', 'BV4', 'BV5','BV6', 'BV7', 'BV8', 'BV9', 'BV10', 'volume', 'amount','volumeIncrease', 'amountIncrease', 'midPrice']
            mycolumns=list(set(data.columns).difference(set(tickColumns)))
            mycolumns=mycolumns+['code', 'date', 'time']
            excludedColumns=['midIncreaseNext1m','midIncreaseNext2m','midIncreaseNext5m','midIncreaseMaxNext1m','midIncreaseMinNext1m','midIncreaseMaxNext2m','midIncreaseMinNext2m','midIncreaseMaxNext5m','midIncreaseMinNext5m','weight50','weight300','weight500','freeMarketValue']
            featuresColumns=list(set(mycolumns).difference(set(excludedColumns)))
            data=data[mycolumns]
            #print(data.shape)
            errorData=data[featuresColumns]
            errorData=errorData[errorData[featuresColumns].isna().sum(axis=1)>0]
            if errorData.shape[0]>0:
                logger.warning(f'factorData of date {day} has Nan!!!')
            #逐日存储数据
            fileName=os.path.join(self.path,str(day)+'.h5')
            exists=os.path.exists(fileName)
            if exists==True:
                os.remove(fileName)
            try:
                with pd.HDFStore(fileName,'a',complib='blosc:zstd',append=True,complevel=9) as store:
                    store.append('data',data,append=True,format="table",data_columns=data.columns)
            except Exception as excp:
                logger.error(f'{fileName} error! {excp}')
            pass
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
    def parallelizationGetDataByDate(self,stockCodes,date,factors=TICKFACTORSUSED):
        data=JobLibUtility.useJobLibToGetFactorDataDaily(self.getLotsDataByDate,stockCodes,MYGROUPS,date,factors)
        return data
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
    #----------------------------------------------------------------------
    def saveLotsDataByDate(self,StockCodes,startDate,endDate):
        for i in range(len(StockCodes)):
            code=StockCodes[i]
            self.saveAllFactorsByCodeAndDays(code,str(startDate),str(endDate))
    #----------------------------------------------------------------------
    def parallelizationSaveDataByDate(self,stockCodes,startDate,endDate):
        JobLibUtility.useJobLibToUpdateData(self.saveLotsDataByDate,stockCodes,MYGROUPS,startDate,endDate)
        pass
    #----------------------------------------------------------------------
    def saveLotsDataToInfluxdbByDate(self,StockCodes,startDate,endDate):
        for i in range(len(StockCodes)):
            code=StockCodes[i]
            self.saveAllFactorsToInfluxdbByCodeAndDays(code,str(startDate),str(endDate))
    #----------------------------------------------------------------------
    def parallelizationSaveDataToInfluxdbByDate(self,stockCodes,startDate,endDate):
        JobLibUtility.useJobLibToUpdateData(self.saveLotsDataToInfluxdbByDate,stockCodes,MYGROUPS,startDate,endDate)
        pass
########################################################################