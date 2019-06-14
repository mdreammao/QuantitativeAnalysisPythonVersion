from Config.myConstant import *
from Config.myConfig import *
from DataAccess.IndexComponentDataProcess import *
from DataAccess.KLineDataProcess import *
from DataAccess.TradedayDataProcess import *
from DataAccess.IndustryClassification import *
from DataAccess.IndexCode import *
from DataAccess.StockSharesProcess import *
from DataAccess.TickDataProcess import *
from DataAccess.TickTransactionDataProcess import TickTransactionDataProcess
from DataPrepare.dataPrepareByIndex import *
from DataPrepare.dailyKLineDataPrepared import *
from Strategy.stockReverseMovement import *
from Strategy.myRandomForestForCeiling import *
from Strategy.stockReverseByStd import *
from Strategy.myAnalysisForReverseByStd import *
from Strategy.stockMomentumByStd import *
from Strategy.myAnalysisForMomentumByStd import *
from Strategy.stockReverseByStdOnTick import *
from Strategy.myAnalysisForReverseByTick import myAnalysisForReverseByTick
from Strategy.myAnalysisForFactorsByDate import myAnalysisForFactorsByDate
from DataPrepare.dailyFactorsProcess import *
from DataPrepare.tickFactorsProcess import *
from Strategy.stockIntradayByTick.reverse.strategy1 import strategy1
from Strategy.stockIntradayByTick.momentum.strategy1 import strategyBreak
from Utility.mytest import *
from Utility.JobLibUtility import *
from Utility.UpdateBasicData import *
import warnings
import time


#----------------------------------------------------------------------
def main():
    """主程序入口"""
    '''
    #industry=IndustryClassification.getIndustryByCode('600251.SH',startDate,endDate)
    #m.xs(m[(((m['open']<m['open'][0]) & (m['time']<='0800'))|(m['time']=='1500')) & (m['date']==m['date'][0])].index[0])
    #mselect=m[(m['open'].shift(1)<m['yesterdayClose']*(1+parameter*m['closeStd20'])) & (m['open']>m['yesterdayClose']*(1+parameter*m['closeStd20']))]
    #mselect=mselect.dropna(axis=0,how='any')
    #myindex=IndexComponentDataProcess()
    #index500=myindex.getCSI500DataByDate(startDate,endDate)
    #index300=myindex.getHS300DataByDate(startDate,endDate)
    #index50=myindex.getSSE50DataByDate(endDate,endDate)
    #stockCodes=list(pd.concat([index500,index300,index50],ignore_index=True)['code'].drop_duplicates())
    '''
    '''
    #性能分析代码
    #python -m cProfile -o del.out XXX.py
    #python -c "import pstats; p=pstats.Stats('del.out'); p.sort_stats('time').print_stats(20)"

    '''
    


    warnings.filterwarnings('ignore')
    startDate=20190101
    endDate=20190610




    UpdateBasicData.updateDailyAll()
    #UpdateBasicData.updateMinuteAll()
    UpdateBasicData.updateTickAll(startDate)
    #UpdateBasicData.updateTickFactorAll(startDate)
    #stockCodes=UpdateBasicData.updateStockCodes(startDate,endDate)
    #UpdateBasicData.updateDailyFactors(stockCodes)
    #----------------------------------------------------------------------
    
    
    '''
    test=strategyBreak()
    codes=list(['000001.SZ','000002.SZ','000006.SZ','000008.SZ','000009.SZ','000012.SZ','000021.SZ','000025.SZ'])
    data=test.multipleCodes_parallel(codes,20190506,20190508)
    print(data)
    #stockCodes=UpdateBasicData.updateStockCodes(startDate,endDate)
    #codes=list(['000001.SZ','000002.SZ','000006.SZ','000008.SZ','000009.SZ','000012.SZ','000021.SZ','000025.SZ'])
    #mydaily=dailyFactorsProcess()
    #mydaily.updateStockDailyFactorsVersion2(codes)
    '''
    #----------------------------------------------------------------------
    '''
    ana=myAnalysisForFactorsByDate('tmp')
    #ana.prepareData(codes,startDate,endDate)
    data=ana.getDataFromLocal(startDate,endDate)
    #data['midIncreaseNext1m']=data['midIncreaseNext1m']/data['midStd60']
    #['code', 'date', 'time', 'buyIncreaseNext1m', 'sellIncreaseNext1m','midIncreaseNext1m', 'midIncreaseNext5m', 'midIncreaseNext10m','midIncreaseNext20m', 'ts_buySellVolumeRatio2','ts_buySellVolumeRatio5', 'ts_buySellVolumeRatio10','buySellVolumeRatio2', 'buySellVolumeRatio5', 'buySellVolumeRatio10',       'differenceHighLow', 'ts_buyForceIncrease', 'ts_sellForceIncrease','ts_buySellForceChange', 'buyForceIncrease', 'sellForceIncrease',       'buySellForceChange', 'midIncreasePrevious3m', 'differenceMidVwap','midStd60', 'ts_midStd60', 'increaseToday', 'closeStd20','ts_closeStd20', 'preClose', 'is300', 'is500', 'buySellSpread','midAbsIncrease1m']
    #data=data[data['is300']==1]
    data['midAbsIncrease1m']=data['midIncreaseNext1m'].abs()
    m=round(data.corr(),3)
    print(m.loc[(m['midIncreaseNext1m'].abs()>=0.03),'midIncreaseNext1m'].sort_values())
    print(m.loc[(m['midAbsIncrease1m'].abs()>=0.03),'midAbsIncrease1m'].sort_values())
    
    #select=(data['ts_buySellForceChange']<=0.05) & (data['ts_buySellVolumeRatio5']<=0.05)& (data['ts_buySellVolumeRatio2']<=0.1) & (data['midIncreasePrevious3m']>0.5*data['closeStd20'])
    #select=(data['ts_buySellForceChange']>=0.95) & (data['ts_buySellVolumeRatio5']>=0.95)& (data['ts_buySellVolumeRatio2']>=0.95) & (data['midIncreasePrevious3m']<-0.5*data['closeStd20'])
    #select=(data['ts_buySellForceChange']>=0.95) & (data['ts_buySellVolumeRatio5']>=0.95)& (data['ts_buySellVolumeRatio2']>=0.95) & (data['midIncreasePrevious3m'].abs()<0.01*data['closeStd20'])  & (data['differenceHighLow']<0.1*data['closeStd20'])
    select=(data['ts_buySellForceChange']<=0.05) & (data['ts_buySellVolumeRatio5']<=0.05)& (data['ts_buySellVolumeRatio2']<=0.05) & (data['midIncreasePrevious3m'].abs()<0.001) & (data['differenceMidVwap'].abs()<0.001)
    x=data[select]
    print(x.shape)
    print(x['midIncreaseNext1m'].mean())
    print(x['midIncreaseNext5m'].mean())
    print(x['midIncreaseNext10m'].mean())
    print(x['midIncreaseNext20m'].mean())
    print(x['buyIncreaseNext1m'].mean())
    print(x['sellIncreaseNext1m'].mean())
    #print(x)
    '''
    #----------------------------------------------------------------------
    '''
    myReverse=stockReverseByStdOnTick()
    print(datetime.datetime.now())
    codes=UpdateBasicData.updateStockCodes(startDate,endDate)
    print(len(codes))
    mydata=myReverse.reverse_multipleCodes(codes,startDate,endDate,[300,100000000,2.5])
    print(datetime.datetime.now())
    print(mydata)
    path=TempLocalFileAddress
    HDF5Utility.pathCreate(path)
    file=os.path.join(TempLocalFileAddress,'reverse.h5')
    with pd.HDFStore(file,'a',complib='blosc:zstd',append=True,complevel=9) as store:
        store.append('data',mydata,append=True,format="table",data_columns=mydata.columns)
    
    myindex=IndexComponentDataProcess()
    index500=myindex.getCSI500DataByDate(startDate,endDate)
    index300=myindex.getHS300DataByDate(startDate,endDate)
    index50=myindex.getSSE50DataByDate(endDate,endDate)
    stockCodes=list(pd.concat([index500,index300,index50],ignore_index=True)['code'].drop_duplicates())
    print(len(stockCodes))
    test=KLineDataProcess('minute')
    print(datetime.datetime.now())
    data=test.parallelizationGetDataByDate(stockCodes,startDate,endDate)
    print(data.shape[0])
    #test.getLotsDataByDate(stockCodes,startDate,endDate)
    print(datetime.datetime.now())
    myindex=IndexComponentDataProcess()
    index500=myindex.getCSI500DataByDate(endDate,endDate)
    index300=myindex.getHS300DataByDate(endDate,endDate)
    index50=myindex.getSSE50DataByDate(endDate,endDate)
    stockCodes=list(pd.concat([index500,index300,index50],ignore_index=True)['code'].drop_duplicates())
    mydata=KLineDataProcess('daily',True)
    mydata.parallelizationGetDataByDate(stockCodes,startDate,endDate)
   
    mytry=dailyFactorsProcess()
    codes=['600000.SH']
    factors=['closeStd','index','marketValue','industry']
    mytry.updateStockDailyFactors(codes,factors)
    '''
    #temp=stockReverseByStd()
    #stockCodes=temp.getStockList(startDate,endDate)
    #temp.dataPrepared(stockCodes,startDate,endDate)
    #temp.parallelizationReverse(startDate,endDate)
    #myanalysis=myAnalysisForReverseByStd()
    #myanalysis.analysis(startDate,endDate)
    #temp=stockMomentumByStd()
    #temp.parallelizationDataPrepared(startDate,endDate)
    #temp.parallelizationMomentum(startDate,endDate) 
    #myanalysis=myAnalysisForMomentumByStd()
    #myanalysis.analysis(startDate,endDate)
    
    pass

if __name__ == '__main__':
    main()
    

