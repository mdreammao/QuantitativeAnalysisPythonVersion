from Config.myConstant import *
from Config.myConfig import *
from DataAccess.IndexComponentDataProcess import *
from DataAccess.KLineDataProcess import *
from DataAccess.TradedayDataProcess import *
from DataAccess.IndustryClassification import *
from DataAccess.IndexCode import *
from DataAccess.StockSharesProcess import *
from DataPrepare.dataPrepareByIndex import *
from DataPrepare.dailyKLineDataPrepared import *
from Strategy.stockReverseMovement import *
from Strategy.myRandomForestForCeiling import *
from Strategy.stockReverseByStd import *
from Strategy.myAnalysisForReverseByStd import *
from Utility.mytest import *
from Utility.JobLibUtility import *
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


    warnings.filterwarnings('ignore')
    startDate=20100101
    endDate=20190415
    temp=stockReverseByStd()
    temp.parallelizationDataPrepared(startDate,endDate)
    temp.parallelizationReverse(startDate,endDate)
    #mytest0=mytest()
    #mytest0.testjoblib(stockCodes,8,startDate,endDate)
    
    

    myanalysis=myAnalysisForReverseByStd()
    myanalysis.analysis(startDate,endDate)

    
    pass
if __name__ == '__main__':
    main()
