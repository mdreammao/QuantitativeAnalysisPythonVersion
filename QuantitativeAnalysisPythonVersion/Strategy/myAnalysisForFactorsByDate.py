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
            #tickColumns=[ 'code', 'date', 'time', 'lastPrice', 'S1', 'S2','S3', 'S4', 'S5', 'S6', 'S7','S8', 'S9', 'S10', 'B1', 'B2', 'B3', 'B4','B5', 'B6', 'B7', 'B8', 'B9', 'B10', 'SV1', 'SV2', 'SV3', 'SV4', 'SV5','SV6', 'SV7', 'SV8', 'SV9', 'SV10', 'BV1', 'BV2', 'BV3', 'BV4', 'BV5','BV6', 'BV7', 'BV8', 'BV9', 'BV10', 'volume', 'amount','volumeIncrease', 'amountIncrease', 'midPrice']
            #dailyColumns=['increaseToday','closeStd20','ts_closeStd20','preClose','is300','is500']
            mycolumns=['code','date','time','buyForce','buySellVolumeRatio5','buySellWeightedVolumeRatio10','buySellVolumeRatio10','sellForce','buyWeightedVolume5','buySellVolumeRatio2', 'buySellSpread','buySellWeightedVolumeRatio2','buySellForceChange','buySellWeightedVolumeRatio5', 'midToVwap', 'midToVwap3m','midPrice3mIncrease','midIncreaseToBV3m', 'midPriceBV3m', 'midInPrevious3m', 'midStd60','increaseToday','closeStd20','ts_closeStd20','preClose','is300','is500','midPriceIncrease','differenceHighLow3m', 'midIncreaseNext2m', 'midIncreaseMaxNext2m','midIncreaseMaxNext5m','midIncreaseMinNext1m', 'midIncreaseMaxNext1m','midIncreaseMinNext2m', 'midIncreaseNext5m','midIncreaseNext1m','midIncreaseMinNext5m']
            data=data[mycolumns]
            #print(data.shape)
            if data[data[mycolumns].isna().sum(axis=1)>0].shape[0]>0:
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
########################################################################
'''
data['midAbsIncrease1m']=data['midIncreaseNext1m'].abs()
m=round(data.corr(),3)
#print(m.loc[(m['midIncreaseNext1m'].abs()>=0.03),'midIncreaseNext1m'].sort_values())
#print(m.loc[(m['midAbsIncrease1m'].abs()>=0.03),'midAbsIncrease1m'].sort_values())
m=f[f.isna().sum(axis=1)>=1]
print(m)
'''