from DataAccess.IndexComponentDataProcess import *
from DataAccess.KLineDataProcess import *
from DataAccess.TradedayDataProcess import *
from DataPrepare.dailyFactorsProcess import *
from DataPrepare.tickFactorsProcess import *
from DataAccess.TickDataProcess import *
from Utility.JobLibUtility import *
import warnings
from Config.myConstant import *
from Config.myConfig import *
import numpy as np
import os
########################################################################
class stockReverseByStdOnTick(object):
    """股票异动,专注股票大涨之后的回调"""
    #----------------------------------------------------------------------
    def __init__(self):
        self.__localFileStr=LocalFileAddress+"\\intermediateResult\\stdFeature.h5"
        self.__localFileStrResult=LocalFileAddress+"\\intermediateResult\\stdReverseResult.h5"
        self.__allMinute=pd.DataFrame()
        self.__key='factorsWithRank'
        self.__factorsAddress=LocalFileAddress+"\\{0}\\{1}.h5".format('dailyFactors',self.__key)
        pass
    
    #----------------------------------------------------------------------
    def reverse(self,code,startDate,endDate):
        days=list(TradedayDataProcess().getTradedays(startDate,endDate))
        factors=['closeStd','index','marketValue','industry']
        dailyRepo=dailyFactorsProcess()
        dailyFactor=dailyRepo.getSingleStockDailyFactors(code,factors,startDate,endDate)
        dailyKLine=KLineDataProcess('daily')
        dailyData=dailyKLine.getDataByDate(code,startDate,endDate)
        tick=TickDataProcess()
        mydata=[]
        for today in days:
            #logger.info(f'{code} in {today} start!')
            todayInfo=dailyFactor[dailyFactor['date']==today]
            todayKLine=dailyData[dailyData['date']==today]
            if (todayInfo.empty==False) & (todayKLine['status'].iloc[0]!='停牌'):
                tickData=tick.getResampleTickShotData(code,today)
                mydata.append(tickData)
                pass
            else:
                logger.warning(f'There is no data of {code} in {today}')
            pass
        mydata=pd.concat(mydata,axis=0)
        file=os.path.join(TempLocalFileAddress,'tick.h5')
        store = pd.HDFStore(path=file,mode='a',complib='blosc:zstd',append=True,complevel=9)
        store.append('data',mydata,append=False)
        store.close()
        print(mydata.shape)
        pass

########################################################################