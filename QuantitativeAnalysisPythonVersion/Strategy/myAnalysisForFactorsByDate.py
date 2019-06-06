from DataAccess.IndexComponentDataProcess import *
from DataAccess.KLineDataProcess import *
from DataAccess.TradedayDataProcess import TradedayDataProcess
from DataPrepare.dailyFactorsProcess import dailyFactorsProcess
from DataPrepare.tickFactorsProcess import tickFactorsProcess
from DataAccess.TickDataProcess import *
from Utility.JobLibUtility import *
from Utility.TradeUtility import *
import warnings
from Config.myConstant import *
from Config.myConfig import *
import numpy as np
import os
import copy
########################################################################
class myAnalysisForFactorsByDate(object):
    """逐日分析tick因子"""
#----------------------------------------------------------------------
    def __init__(self,document='analysis'):
        self.path=os.path.join(LocalFileAddress,document)
        HDF5Utility.pathCreate(self.path)
        pass
#----------------------------------------------------------------------
    def getDataFromLocalDaily(self,today):
        fileName=os.path.join(self.path,str(today)+'.h5')
        data=pd.DataFrame()
        try:
            with pd.HDFStore(fileName,'r',complib='blosc:zstd',append=True,complevel=9) as store:
                data=store['data']
        except Exception as excp:
            logger.error(f'{fileName} error! {excp}')
            pass
        return data
#----------------------------------------------------------------------
    def getDataFromLocal(self,startDate,endDate):
        tradedays=TradedayDataProcess.getTradedays(startDate,endDate)
        tradedays=list(tradedays)
        with parallel_backend("multiprocessing", n_jobs=JobLibUtility.myjobs):
            mydata=Parallel()(delayed(self.getDataFromLocalDaily)(tradedays[i]) for i in range(len(tradedays)))
        all=pd.concat(mydata)
        '''
        for day in tradedays:
            fileName=os.path.join(self.path,str(day)+'.h5')
            try:
                with pd.HDFStore(fileName,'r',complib='blosc:zstd',append=True,complevel=9) as store:
                    data=store['data']
                    all.append(data)
            except Exception as excp:
                logger.error(f'{fileName} error! {excp}')
                pass
        all=pd.concat(all)
        '''
        return all
        pass
#----------------------------------------------------------------------
    def prepareData(self,codeList,startDate,endDate):
        tradedays=TradedayDataProcess.getTradedays(startDate,endDate)
        myfactor=tickFactorsProcess()
        for day in tradedays:
            data=myfactor.parallelizationGetDataByDate(codeList,day)
            data=data[(data['time']>='093500000') & (data['time']<='145000000')]
            data=data[(data['SV1']>0) & (data['BV1']>0)]
            mycolumns=[ 'code', 'date', 'time','buyIncreaseNext1m','sellIncreaseNext1m','midIncreaseNext1m', 'midIncreaseNext5m','midIncreaseNext10m','midIncreaseNext20m','ts_buySellVolumeRatio2','ts_buySellVolumeRatio5','ts_buySellVolumeRatio10','buySellVolumeRatio2','buySellVolumeRatio5','buySellVolumeRatio10','differenceHighLow','ts_buyForceIncrease','ts_sellForceIncrease','ts_buySellForceChange','buyForceIncrease','sellForceIncrease','buySellForceChange','midIncreasePrevious3m','differenceMidVwap','midStd60','ts_midStd60','increaseToday','closeStd20','ts_closeStd20','preClose','is300','is500','buySellSpread']
            features=[ 'ts_buySellVolumeRatio2','ts_buySellVolumeRatio5','ts_buySellVolumeRatio10','buySellVolumeRatio2','buySellVolumeRatio5','buySellVolumeRatio10','differenceHighLow','ts_buyForceIncrease','ts_sellForceIncrease','ts_buySellForceChange','buyForceIncrease','sellForceIncrease','buySellForceChange','midIncreasePrevious3m','differenceMidVwap','midStd60','ts_midStd60','increaseToday','closeStd20','ts_closeStd20','preClose','buySellSpread']
            data=data[mycolumns]
            data=data[data[features].isna().sum(axis=1)==0]
            print(data.shape)
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
########################################################################
'''
data['midAbsIncrease1m']=data['midIncreaseNext1m'].abs()
m=round(data.corr(),3)
#print(m.loc[(m['midIncreaseNext1m'].abs()>=0.03),'midIncreaseNext1m'].sort_values())
#print(m.loc[(m['midAbsIncrease1m'].abs()>=0.03),'midAbsIncrease1m'].sort_values())
m=f[f.isna().sum(axis=1)>=1]
print(m)
'''