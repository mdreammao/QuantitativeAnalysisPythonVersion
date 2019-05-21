from Config.myConstant import *
from Config.myConfig import *
from DataAccess.IndexComponentDataProcess import *
from DataAccess.KLineDataProcess import *
from DataAccess.TradedayDataProcess import *
from DataAccess.IndustryClassification import *
from DataAccess.IndexCode import *
from DataAccess.StockSharesProcess import *
from DataAccess.TickDataProcess import *
from DataPrepare.dataPrepareByIndex import *
from DataPrepare.dailyKLineDataPrepared import *
from Strategy.stockReverseMovement import *
from Strategy.myRandomForestForCeiling import *
from Strategy.stockReverseByStd import *
from Strategy.myAnalysisForReverseByStd import *
from Strategy.stockMomentumByStd import *
from Strategy.myAnalysisForMomentumByStd import *
from Strategy.stockReverseByStdOnTick import *
from DataPrepare.dailyFactorsProcess import *
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
    endDate=20190515
    myReverse=stockReverseByStdOnTick()
    print(datetime.datetime.now())
    codes=['600000.SH','000001.SZ']
    mydata=myReverse.reverse_multipleCodes(codes,startDate,endDate)
    print(datetime.datetime.now())
    print(mydata)
    '''
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
