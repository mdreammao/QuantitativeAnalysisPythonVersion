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
from DataAccess.TickDataProcess import TickDataProcess
########################################################################
class gradeStrategy1(baseStrategy):
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
            #tickData=tick.getDataByDateFromLocalFile(code,day)
            tickData=tick.getTickShotDataFromInfluxdbServer(code,day)
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
            A=self.dataSelect(A,0.2)
            A=A.values
            maxWeight=np.array([ 0.03218688, -0.0121024 , -0.00970715,  0.48172206,  0.42610642,
        0.10048948, -0.05574053,  0.08212702, -0.12357012, -0.00123216,
        0.09529259,  0.00509518,  0.14970625, -0.00291313,  0.00402094,
       -0.00452788,  0.00286216,  0.0020172 , -0.00235546])
            minWeight=np.array([-0.00385887, -0.01163938,  0.0043455 , -0.01114819, -0.34286923,
        0.08314041,  0.00154458,  0.12249813, -0.02194375, -0.00038749,
       -0.02217015,  0.00610296, -0.09264385, -0.0020065 ,  0.00249547,
       -0.00324293,  0.00501176,  0.00389697, -0.00294958])
            maxIntercept=0.00079871
            minIntercept=-0.00155935
            mymax=A.dot(maxWeight)+maxIntercept
            mymin=A.dot(minWeight)+minIntercept
            data['maxPredict']=mymax
            data['minPredict']=mymin
            data['maxPredict']=data['maxPredict'].ewm(span=2,ignore_na=True, adjust= True).mean()
            data['minPredict']=data['minPredict'].ewm(span=2,ignore_na=True, adjust= True).mean()
            data['midPredict']=(data['maxPredict']+data['minPredict'])/2
            m=data[['midIncreaseMinNext5m','midIncreaseMaxNext5m','maxPredict','minPredict','midPredict']]
            print(m.corr())
            #long=data[(data['maxPredict']>0.01)]['midIncreaseMaxNext5m'].mean()-data['midIncreaseMaxNext5m'].mean()
            #short=data[(data['minPredict']<-0.01)]['midIncreaseMinNext5m'].mean()-data['midIncreaseMinNext5m'].mean()
            #print(long)
            #print(short)
            mycolumns=list(tickData.columns)
            mycolumns.append('maxPredict')
            mycolumns.append('minPredict')
            data=data[mycolumns]
            parameters={'maxPosition':maxPosition,'longOpen':0.015,'shortOpen':-0.015,'longClose':0.01,'shortClose':-0.01,'transactionRatio':0.2}
            #trade0=self.strategy(data,parameters)
            #trade.append(trade0)
            pass
        if len(trade)==0:
            trade=pd.DataFrame()
        else:
            trade=pd.concat(trade)
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
        
        trade=[]
        dict={}
        for i in range(len(mydata)):
           tick=mydata[i] 
           maxPredict=tick[myindex['maxPredict']]
           minPredict=tick[myindex['minPredict']]
           S1=tick[myindex['S1']]
           B1=tick[myindex['B1']]
           SV1=tick[myindex['SV1']]
           BV1=tick[myindex['BV1']]
           time=tick[myindex['time']]
           date=tick[myindex['date']]
           if (unusedPosition>0) &  (time<'145000000'):#仍然可以开仓
               if (maxPredict>0.01) & (minPredict>-0.005) & (SV1>=100/transactionRatio):
                   transactionVolume=min(round(transactionRatio*SV1,-2),unusedPosition)
                   unusedPosition=unusedPosition-transactionVolume
                   todayPosition=todayPosition+transactionVolume
                   transactionPrice=S1
                   transactionTime=time
                   transactionDate=date
                   dict={'date':transactionDate,'time':transactionTime,'direction':'buy','volume':transactionVolume,'price':transactionPrice}
                   trade.append(dict)
                   pass
               elif (maxPredict<0.004) & (minPredict<-0.008) & (BV1>=100/transactionRatio):
                   transactionVolume=min(round(transactionRatio*BV1,-2),unusedPosition)
                   unusedPosition=unusedPosition-transactionVolume
                   todayPosition=todayPosition-transactionVolume
                   transactionPrice=B1
                   transactionTime=time
                   transactionDate=date
                   dict={'date':transactionDate,'time':transactionTime,'direction':'sell','volume':transactionVolume,'price':transactionPrice}
                   trade.append(dict)
                   pass
           if todayPosition!=0:
               if todayPosition>0:#持有多仓
                   if (maxPredict<0.005) | (time>='145500000'):
                       [averagePrice,sellPosition]=super().sellByTickShotData(tick,myindex,abs(todayPosition),0.05)
                       transactionVolume=sellPosition
                       todayPosition=todayPosition-transactionVolume
                       transactionPrice=averagePrice
                       transactionTime=time
                       transactionDate=date
                       dict={'date':transactionDate,'time':transactionTime,'direction':'sell','volume':transactionVolume,'price':transactionPrice}
                       trade.append(dict)
                       pass
                   pass
               elif todayPosition<0:#持有空仓
                   if (minPredict>-0.005)|(time>='145500000'):
                       [averagePrice,buyPosition]=super().buyByTickShotData(tick,myindex,abs(todayPosition),0.05)
                       transactionVolume=buyPosition
                       todayPosition=todayPosition+transactionVolume
                       transactionPrice=averagePrice
                       transactionTime=time
                       transactionDate=date
                       dict={'date':transactionDate,'time':transactionTime,'direction':'buy','volume':transactionVolume,'price':transactionPrice}
                       trade.append(dict)
                       pass
                   pass
        trade=pd.DataFrame(data=trade)    
        #print(trade)
        return trade
########################################################################