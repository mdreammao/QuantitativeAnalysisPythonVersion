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
from MachineLearning.RNN.RNN001 import RNN001
from MachineLearning.XGBoost.xgboost001 import xgboost001
from Strategy.baseStrategy.grade.gradeStrategy1 import gradeStrategy1
from Strategy.baseStrategy.grade.gradeStrategyXgboost import gradeStrategyXgboost
from Strategy.baseStrategy.grade.gradeStrategyDNN import gradeStrategyDNN
from DataPrepare.tickFactors.tickDataPrepared import tickDataPrepared
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
    logger.info(f'compute start!!!')
    startDate=20160101
    endDate=20160110




    #UpdateBasicData.updateDailyAll()
    #UpdateBasicData.updateMinuteAll()
    #UpdateBasicData.updateTickAll(startDate)
    #UpdateBasicData.updateTickFactorAll(startDate)
    #stockCodes=UpdateBasicData.updateStockCodes(startDate,endDate)
    #UpdateBasicData.updateDailyFactors(stockCodes)
    #----------------------------------------------------------------------
    stockCodes=['000001.SZ']
    UpdateBasicData.updateMultipleStocksTickShots(stockCodes,startDate,endDate)
    UpdateBasicData.updateMultipleStocksTickFactors(stockCodes,startDate,endDate)
    #tick=tickDataPrepared()
    #tick.saveAllFactorsToInfluxdbByCodeAndDay('000001.SZ',20190105)
    pass

if __name__ == '__main__':
    main()
    

