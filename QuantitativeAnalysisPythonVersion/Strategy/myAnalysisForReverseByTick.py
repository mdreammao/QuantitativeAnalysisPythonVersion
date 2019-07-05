from DataAccess.IndexComponentDataProcess import *
from DataAccess.KLineDataProcess import *
from DataAccess.TradedayDataProcess import *
from DataPrepare.dailyFactorsProcess import *
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
class myAnalysisForReverseByTick(object):
    """股票异动,专注股票大涨之后的回调"""
    #----------------------------------------------------------------------
    def __init__(self):
        pass
    #----------------------------------------------------------------------
    def reverse_multipleCodes_parallel(self,codes,startDate,endDate):
        mydata=JobLibUtility.useJobLibToComputeByCodes(self.reverse_multipleCodes,codes,MYGROUPS,startDate,endDate)
        return mydata
        pass
    #----------------------------------------------------------------------
    #输入code=600000.SH，startdate=yyyyMMdd，endDate=yyyyMMdd
    def reverse_multipleCodes(self,codes,startDate,endDate):
        mydata=[]
        for i in range(len(codes)):
            code=codes[i]
            data=self.reverse_singleCode(code,startDate,endDate)
            mydata.append(data)
        mydata=pd.concat(mydata)
        return mydata
    #----------------------------------------------------------------------
    def reverse_singleCode(self,code,startDate,endDate):
        days=list(TradedayDataProcess().getTradedays(startDate,endDate))
        #factors=['closeStd','index','marketValue','industry']
        dailyRepo=dailyFactorsProcess()
        dailyFactor=dailyRepo.getSingleStockDailyFactors(code,startDate,endDate)
        dailyKLine=KLineDataProcess('daily')
        dailyData=dailyKLine.getDataByDate(code,startDate,endDate)
        if dailyData.empty==True:
            logger.error(f'there is no data of {code} from {startDate} to {endDate}')
            return pd.DataFrame()
        tick=TickDataProcess()
        tickfactor=tickFactorsProcess()
        mydata=[]
        position=0
        profit=0
        myStatusList=[]
        myTradeList=[]
        myStatus={}
        myTrade={}
        positionYesterday=0
        select1=[]
        select2=[]
        selectall=[]
        for today in days:
            #logger.info(f'{code} in {today} start!')
            todayInfo=dailyFactor[dailyFactor['date']==today]
            if todayInfo.empty==True:
                logger.error(f'there is no factor data of {code} in date {today}')
                continue
                pass
            todayKLine=dailyData[dailyData['date']==today]
            if todayKLine.empty==True:
                logger.error(f'there is no KLine data of {code} in date {today}')
                continue
                pass
            myStatus['date']=today
            myStatus['closeStd20']=todayInfo['closeStd20'].iloc[0]
            myStatus['weight50']=todayInfo['weight50'].iloc[0]
            myStatus['weight300']=todayInfo['weight300'].iloc[0]
            myStatus['weight500']=todayInfo['weight500'].iloc[0]
            myStatus['ts_closeStd20']=todayInfo['closeStd20'].iloc[0]
            myStatus['adjFactor']=todayKLine['adjFactor'].iloc[0]
            myStatus['preClose']=todayKLine['preClose'].iloc[0]
            positionNow=positionYesterday
            if (todayInfo.empty==False) & (todayKLine['status'].iloc[0]!='停牌'):
                #tickData=tick.getDataByDateFromLocalFile(code,today)
                tickData=tick.getTickShotDataFromInfluxdbServer(code,today)
                #factors=tickfactor.getDataByDateFromLocalFile(code,today,'buySellForce')
                factors=tickfactor.getFactorsUsedByDateFromLocalFile(code,today)
                all=pd.merge(tickData,factors,how='left',left_index=True,right_index=True)
                all['closeStd20']=todayInfo['closeStd20'].iloc[0]
                all['ts_closeStd20']=todayInfo['ts_closeStd20'].iloc[0]
                all['adjFactor']=todayKLine['adjFactor'].iloc[0]
                all['preClose']=todayKLine['preClose'].iloc[0]
                all['increaseToday']=all['midPrice']/all['preClose']-1
                all['midIncreasePrevious5m']=all['midPrice']/all['midPrice'].shift(60)-1
                #select0=all[all['increaseToday']>2*all['closeStd20']]
                selectall.append(all)
                select0=all[all['midIncreasePrevious5m']>0.5*all['closeStd20']]
                select1.append(select0)
                select0=all[all['midIncreasePrevious5m']<-0.5*all['closeStd20']]
                select2.append(select0)
                pass
            else:
                logger.warning(f'There is no data of {code} in {today}')
        mycolumns=['midIncreaseNext1m','midIncreaseNext5m','ts_buyForceIncrease','ts_sellForceIncrease','ts_buySellVolumeRatio5','midIncreasePrevious3m','differenceHighLow','ts_differenceHighLow','differenceMidVwap','ts_differenceMidVwap','midStd60','ts_midStd60']
        select1=pd.concat(select1)
        select2=pd.concat(select2)
        selectall=pd.concat(selectall)
        selectall=selectall[mycolumns]
        s=select1[(select1['ts_sellForceIncrease']>0.6) &(select1['ts_buySellVolumeRatio5']>0.6)& (select1['increaseToday']>select1['closeStd20'])]
        print(s['midIncreaseNext5m'].mean())
        ss=select2[(select2['ts_buySellVolumeRatio5']>0.6)& (select2['increaseToday']<-select2['closeStd20'])]
        print(ss['midIncreaseNext5m'].mean())
        return mydata
########################################################################