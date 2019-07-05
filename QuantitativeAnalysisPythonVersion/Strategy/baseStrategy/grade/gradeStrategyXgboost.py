from Config.myConstant import *
from Config.myConfig import *
from DataAccess.TradedayDataProcess import TradedayDataProcess
from DataPrepare.tickFactorsProcess import tickFactorsProcess
from DataAccess.KLineDataProcess import KLineDataProcess
from DataPrepare.dailyFactorsProcess import dailyFactorsProcess
from Utility.JobLibUtility import JobLibUtility
from typing import List as type_list
from Strategy.baseStrategy.baseStrategy import baseStrategy
import pandas as pd
import numpy as np
import math
import xgboost as xgb
import warnings
from DataAccess.TickDataProcess import TickDataProcess
########################################################################
class gradeStrategyXgboost(baseStrategy):
    """按照打分交易"""
    #----------------------------------------------------------------------
    def __init__(self):
        self.name='按照多因子打分进行交易'
        pass
    #----------------------------------------------------------------------
    def multipleCodes_parallel(self,codes:type_list[str],startDate:str,endDate:str,parameters=[]):
        mydata=JobLibUtility.useJobLibToComputeByCodes(self.multipleCodes,codes,MYGROUPS,startDate,endDate,parameters)
        return mydata
        pass
    #----------------------------------------------------------------------
    #输入code=600000.SH，startdate=yyyyMMdd，endDate=yyyyMMdd
    def multipleCodes(self,codes:type_list[str],startDate:str,endDate:str,parameters=[]):
        mydata=[]
        for i in range(len(codes)):
            code=codes[i]
            data=self.singleCode(code,startDate,endDate,parameters)
            mydata.append(data)
        mydata=pd.concat(mydata)
        return mydata
    #----------------------------------------------------------------------
    def dataSelect(self,data,c):
        pd.set_option('mode.use_inf_as_na', True) 
        data=data[data.isna().sum(axis=1)==0]
        select=data['buyForce']>c
        data.loc[select,'buyForce']=c
        select=data['sellForce']>c
        data.loc[select,'sellForce']=c
        select=data['buySellForceChange']>c
        data.loc[select,'buySellForceChange']=c
        select=data['buySellForceChange']<-c
        data.loc[select,'buySellForceChange']=-c
        return data
        pass
    #----------------------------------------------------------------------
    def singleCode(self,code:str,startDate:str,endDate:str,parameters=[]):
        days=list(TradedayDataProcess().getTradedays(startDate,endDate))
        tickFactors=tickFactorsProcess()
        tick=TickDataProcess()
        daily=dailyFactorsProcess()
        dailyKLine=KLineDataProcess('daily')
        trade=[]
        for day in days:
            tickData=tick.getDataByDateFromLocalFile(code,day)
            if tickData.shape[0]==0:
                continue
            data=tickFactors.getTickFactorsOnlyByDateFromLocalFile(code,day)
            data=pd.merge(data,tickData,how='left',left_index=True,right_index=True)
            dailyData=daily.getSingleStockDailyFactors(code,day,day)
            for col in dailyData.columns:
                if col not in ['date','code']:
                    data[col]=dailyData[col].iloc[0]
            dailyKLineData=dailyKLine.getDataByDate(code,day,day)
            data['preClose']=dailyKLineData['preClose'].iloc[0]
            data['increaseToday']=data['midPrice']/data['preClose']-1
            if np.isnan(data['weight300'].iloc[0])==True:
                continue
            maxPosition=round(data['weight300'].iloc[0]*100000000/data['preClose'].iloc[0]/100,-2)
            features=['buyForce','sellForce','buySellForceChange','buySellSpread',
                'differenceHighLow3m','midToVwap','midToVwap3m','midPrice3mIncrease','midPriceBV3m', 'midInPrevious3m', 
                'midStd60', 'increaseToday','closeStd20',
                'buySellVolumeRatio2', 'buySellWeightedVolumeRatio2',
                'buySellVolumeRatio5','buySellWeightedVolumeRatio5',
                 'buySellVolumeRatio10','buySellWeightedVolumeRatio10'
               ]
            A=data[features]
            #A=self.dataSelect(A,0.2)
            A=A.values
            warnings.filterwarnings('ignore')
            factors=xgb.DMatrix(A)
            file=os.path.join(LocalFileAddress,'tmp','xgb001_midIncreaseMaxNext5m.model')
            model=xgb.Booster(model_file=file)
            mymax=model.predict(factors)
            file=os.path.join(LocalFileAddress,'tmp','xgb001_midIncreaseMinNext5m.model')
            model=xgb.Booster(model_file=file)
            mymin=model.predict(factors)
            data['maxPredict']=mymax
            data['minPredict']=mymin
            data['maxPredict']=data['maxPredict'].ewm(span=2,ignore_na=True, adjust= True).mean()
            data['minPredict']=data['minPredict'].ewm(span=2,ignore_na=True, adjust= True).mean()
            data['midPredict']=(data['maxPredict']+data['minPredict'])/2
            m=data[['midIncreaseMinNext5m','midIncreaseMaxNext5m','maxPredict','minPredict','midPredict']]
            pd.set_option('display.max_rows',None)
            #print(m.corr())
            #long=data[(data['maxPredict']>0.003)]['midIncreaseMaxNext5m'].mean()-data['midIncreaseMaxNext5m'].mean()
            #short=data[(data['minPredict']<-0.003)]['midIncreaseMinNext5m'].mean()-data['midIncreaseMinNext5m'].mean()
            #print(long)
            #print(short)
            mycolumns=list(tickData.columns)
            mycolumns.append('maxPredict')
            mycolumns.append('minPredict')
            mycolumns.append('midPredict')
            mycolumns.append('increaseToday')
            data=data[mycolumns]
            parameters={'maxPosition':maxPosition,'longOpen':0.015,'shortOpen':-0.015,'longClose':0.01,'shortClose':-0.01,'transactionRatio':0.1}
            trade0=self.strategy(data,parameters)
            trade.append(trade0)
            pass
        if len(trade)==0:
            trade=pd.DataFrame()
        else:
            trade=pd.concat(trade)
            if trade.shape[0]==0:
                return pd.DataFrame()
            trade['code']=code
            trade['fee']=trade['price']*0.0001
            selectBuy=trade['direction']=='buy'
            selectSell=trade['direction']=='sell'
            trade.loc[selectSell,'fee']=(trade['fee']+trade['price']*0.001)[selectSell]
            trade.loc[selectBuy,'cashChange']=((-trade['price']-trade['fee'])*trade['volume'])[selectBuy]
            trade.loc[selectSell,'cashChange']=((trade['price']-trade['fee'])*trade['volume'])[selectSell]
            trade['amount']=trade['price']*trade['volume']
        return trade
        pass

    #----------------------------------------------------------------------
    def strategy(self,data:pd.DataFrame,parameters):
        maxPosition=parameters['maxPosition']
        longOpen=parameters['longOpen']
        longClose=parameters['longClose']
        shortOpen=parameters['shortOpen']
        shortClose=parameters['shortClose']
        transactionRatio=parameters['transactionRatio']
        unusedPosition=maxPosition
        todayPosition=0
        myindex={}
        select=list(data.columns)
        for item in data.columns:
            myindex.update({item:select.index(item)})
        mydata=data.values
        openPrice=0
        stop=False
        trade=[]
        dict={}
        for i in range(len(mydata)):
           tick=mydata[i] 
           maxPredict=tick[myindex['maxPredict']]
           minPredict=tick[myindex['minPredict']]
           midPredict=tick[myindex['midPredict']]
           S1=tick[myindex['S1']]
           B1=tick[myindex['B1']]
           SV1=tick[myindex['SV1']]
           BV1=tick[myindex['BV1']]
           time=tick[myindex['tick']]
           date=tick[myindex['date']]
           increaseToday=tick[myindex['increaseToday']]
           #止盈止损
           if todayPosition>0:
               if (B1-openPrice)/openPrice<=-0.01:
                   stop=True
               #if (B1-openPrice)/openPrice>=0.005:
               #    stop=True
               pass
           elif todayPosition<0:
               if (openPrice-S1)/openPrice<=-0.01:
                   stop=True
               #if (openPrice-S1)/openPrice>=0.005:
               #   stop=True
               pass
           if (unusedPosition>0) &  (time<'145000000'):#仍然可以开仓
               if (midPredict>0.0015) &(maxPredict>0.003) &  (SV1>=100/transactionRatio) & (increaseToday>-0.09):
                   transactionVolume=min(round(transactionRatio*SV1,-2),unusedPosition)
                   unusedPosition=unusedPosition-transactionVolume
                   todayPosition=todayPosition+transactionVolume
                   transactionPrice=S1
                   transactionTime=time
                   transactionDate=date
                   dict={'date':transactionDate,'tick':transactionTime,'direction':'buy','volume':transactionVolume,'price':transactionPrice}
                   trade.append(dict)
                   if openPrice==0:
                       openPrice=transactionPrice
                   pass
               elif (midPredict<-0.0015) & (minPredict<-0.003)&(minPredict<-1*maxPredict) & (BV1>=100/transactionRatio)& (increaseToday<0.09):
                   transactionVolume=min(round(transactionRatio*BV1,-2),unusedPosition)
                   unusedPosition=unusedPosition-transactionVolume
                   todayPosition=todayPosition-transactionVolume
                   transactionPrice=B1
                   transactionTime=time
                   transactionDate=date
                   dict={'date':transactionDate,'tick':transactionTime,'direction':'sell','volume':transactionVolume,'price':transactionPrice}
                   trade.append(dict)
                   if openPrice==0:
                       openPrice=transactionPrice
                   pass
           if todayPosition!=0:
               if todayPosition>0:#持有多仓
                   if (stop==True) |(midPredict<-0.0004)| (time>='145500000') | (increaseToday<-0.095):
                       [averagePrice,sellPosition]=super().sellByTickShotData(tick,myindex,abs(todayPosition),0.05)
                       transactionVolume=sellPosition
                       todayPosition=todayPosition-transactionVolume
                       transactionPrice=averagePrice
                       transactionTime=time
                       transactionDate=date
                       dict={'date':transactionDate,'tick':transactionTime,'direction':'sell','volume':transactionVolume,'price':transactionPrice}
                       trade.append(dict)
                       if todayPosition==0:
                           openPrice==0
                           stop=False
                       pass
                   pass
               elif todayPosition<0:#持有空仓
                   if (stop==True)|(midPredict>0.0004)|(time>='145500000')| (increaseToday>0.095):
                       [averagePrice,buyPosition]=super().buyByTickShotData(tick,myindex,abs(todayPosition),0.05)
                       transactionVolume=buyPosition
                       todayPosition=todayPosition+transactionVolume
                       transactionPrice=averagePrice
                       transactionTime=time
                       transactionDate=date
                       dict={'date':transactionDate,'tick':transactionTime,'direction':'buy','volume':transactionVolume,'price':transactionPrice}
                       trade.append(dict)
                       if todayPosition==0:
                           openPrice==0
                           stop=False
                       pass
                   pass
        trade=pd.DataFrame(data=trade)    
        #print(trade)
        return trade
########################################################################