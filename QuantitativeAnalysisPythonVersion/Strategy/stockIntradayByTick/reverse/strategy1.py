from Config.myConstant import *
from Config.myConfig import *
from DataAccess.TradedayDataProcess import TradedayDataProcess
from DataPrepare.tickFactorsProcess import tickFactorsProcess
from Utility.JobLibUtility import JobLibUtility
import pandas as pd
########################################################################
class strategy1(object):
    """description of class"""
    #----------------------------------------------------------------------
    def __init__(self):
        pass
    #----------------------------------------------------------------------
    def multipleCodes_parallel(self,codes,startDate,endDate,parameters=[]):
        mydata=JobLibUtility.useJobLibToComputeByCodes(self.multipleCodes,codes,MYGROUPS,startDate,endDate,parameters)
        return mydata
        pass
    #----------------------------------------------------------------------
    #输入code=600000.SH，startdate=yyyyMMdd，endDate=yyyyMMdd
    def multipleCodes(self,codes,startDate,endDate,parameters=[]):
        mydata=[]
        for i in range(len(codes)):
            code=codes[i]
            data=self.singleCode(code,startDate,endDate,parameters=[])
            mydata.append(data)
        mydata=pd.concat(mydata)
        return mydata
    #----------------------------------------------------------------------
    def singleCode(self,code,startDate,endDate,parameters):
        days=list(TradedayDataProcess().getTradedays(startDate,endDate))
        tick=tickFactorsProcess()
        select=['code','date','time','B1','S1','midIncreasePrevious3m','closeStd20','ts_closeStd20','ts_buySellForceChange','ts_buySellVolumeRatio5','ts_buySellVolumeRatio2','preClose','increaseToday','weight300','weight500']
        trade=[]
        for day in days:
            data=tick.getTickDataAndFactorsByDateFromLocalFile(code,day)
            if data.shape[0]>0:
                trade0=self.__strategy(data,select)
                trade.append(trade0)
            pass
        if trade==[]:
            trade=pd.DataFrame()
        else:
            trade=pd.concat(trade)
        return trade
        pass
    #----------------------------------------------------------------------
    def __strategy(self,data,select):
        mydata=data[select].copy()
        myindex={}
        for item in select:
            myindex.update({item:select.index(item)})
        mydata=mydata.as_matrix()
        position=0
        startIndex=0
        trade=[]
        dict={}
        for i in range(len(mydata)):
            mytick=mydata[i]
            mytime=mytick[myindex['time']]
            increaseToday=mytick[myindex['increaseToday']]
            weight300=mytick[myindex['weight300']]
            weight500=mytick[myindex['weight500']]
            #开盘5分钟不操作
            if mytime<='093500000':
                continue
            if (position==0) & ((increaseToday>0.09) | (increaseToday<-0.09)):
                continue
            ts_buySellForceChange=mytick[myindex['ts_buySellForceChange']]
            ts_buySellVolumeRatio5=mytick[myindex['ts_buySellVolumeRatio5']]
            ts_buySellVolumeRatio2=mytick[myindex['ts_buySellVolumeRatio2']]
            midIncreasePrevious3m=mytick[myindex['midIncreasePrevious3m']]
            closeStd20=mytick[myindex['closeStd20']]
            if (position==0)& (ts_buySellForceChange>=0.8) & (ts_buySellVolumeRatio5>=0.8) & (ts_buySellVolumeRatio2>=0.8) & (midIncreasePrevious3m<-0.5*closeStd20):
                if (ts_buySellForceChange>=0.8) & (ts_buySellVolumeRatio5>=0.8) & (ts_buySellVolumeRatio2>=0.8) & (midIncreasePrevious3m<-0.5*closeStd20):
                    open=mytick[myindex['S1']]
                    position=1
                    startIndex=i
                    dict={'open':open,'openTime':mytime,'date':mytick[myindex['date']],'code':mytick[myindex['code']],'position':position,'weight300':weight300,'weight500':weight500}
                    pass
                elif (ts_buySellForceChange<=0.2) & (ts_buySellVolumeRatio5<=0.2) & (ts_buySellVolumeRatio2<=0.2) & (midIncreasePrevious3m>0.5*closeStd20):
                    open=mytick[myindex['B1']]
                    startIndex=i
                    position=-1
                    dict={'open':open,'openTime':mytime,'date':mytick[myindex['date']],'code':mytick[myindex['code']],'position':position,'weight300':weight300,'weight500':weight500}
                    pass
            elif position==1:
                close=mytick[myindex['B1']]
                yieldNow=(close-open)/open
                hold=i-startIndex
                if ((ts_buySellForceChange<=0.3) & (ts_buySellVolumeRatio5<=0.3) & (ts_buySellVolumeRatio2<=0.3) )|(mytime>='145500000') |(increaseToday<-0.09) | (yieldNow<-0.01) :
                    position=0
                    dict.update({'close':close,'closeTime':mytime,'yield':yieldNow})
                    trade.append(dict)
                    break
                    pass
            elif position==-1 :
                close=mytick[myindex['S1']]
                yieldNow=-(close-open)/open
                hold=i-startIndex
                if ((ts_buySellForceChange>=0.3) & (ts_buySellVolumeRatio5>=0.3) & (ts_buySellVolumeRatio2>=0.3)) |(mytime>='145500000') |(increaseToday>0.09) |(yieldNow<-0.01):
                    position=0
                    dict.update({'close':close,'closeTime':mytime,'yield':yieldNow})
                    trade.append(dict)
                    break
        trade=pd.DataFrame(data=trade)      
        return trade
########################################################################