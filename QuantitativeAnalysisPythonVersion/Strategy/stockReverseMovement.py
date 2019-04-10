from DataAccess.IndexComponentDataProcess import *
from DataAccess.KLineDataProcess import *
from DataAccess.TradedayDataProcess import *
from Config.myConstant import *
from Config.myConfig import *


########################################################################
class stockReverseMovement(object):
    """股票异动,专注股票大涨之后的回调"""
    #----------------------------------------------------------------------
    def __init__(self):
        self.__myMinute=KLineDataProcess('minute',True)
        self.__myDaily=KLineDataProcess('daily',True)
        self.__localFileStr=LocalFileAddress+"\\intermediateResult\\ceilingFeature.h5"
        self.__allMinute=pd.DataFrame()
        pass
    #----------------------------------------------------------------------
    def __getStockList(self):
        myindex=IndexComponentDataProcess()
        index500=myindex.getCSI500DataByDate(20190404,20190404)
        index300=myindex.getHS300DataByDate(20190404,20190404)
        index50=myindex.getSSE50DataByDate(20190404,20190404)
        stockCodes=list(pd.concat([index500,index300,index50],ignore_index=True)['code'].drop_duplicates())
        return stockCodes
    #----------------------------------------------------------------------
    def __dataPrepared(self):
        mylist=self.__getStockList()
        self.__minuteData={}
        self.__dailyData={}
        num=0
        for code in mylist:
            num=num+1
            print("{0}({1} of 50) start!".format(code,num))
            print(datetime.datetime.now())
            self.__dataSelectOneByOne(code)
            d=self.__dailyData[code]
            m=self.__minuteData[code]
            d['ceiling']=0
            d['ceilingYesterday']=0
            d['ceilingYesterday2']=0
            d['ceilingIn5Days']=0
            d.loc[(d['close']==round(d['preClose']*1.1,2)),'ceiling']=1
            d.loc[(d['ceiling'].shift(1)==1),'ceilingYesterday']=1
            d.loc[((d['ceiling'].shift(1)==1) & (d['ceiling'].shift(2)==1)),'ceilingYesterday2']=1
            d['ceilingIn5Days']=d['ceilingYesterday'].rolling(5).sum()
            #print(d[d['ceilingYesterday']>0][['date','close','preClose','ceilingIn5Days']])
            m.loc[(m['date']==m['date'].shift(5)),'increase5m']=(m['open']/m['open'].shift(5)-1)
            m.loc[(m['date']==m['date'].shift(1)),'increase1m']=(m['open']/m['open'].shift(1)-1)
            d=d.set_index('date')
            dailyInfo=d.loc[m['date'],['preClose','ceilingYesterday','ceilingYesterday2','ceilingIn5Days']]
            dailyInfo.index=m.index
            m[['yesterdayClose','ceilingYesterday','ceilingYesterday2','ceilingIn5Days']]=dailyInfo
            m['increaseInDay']=(m['open']/m['yesterdayClose']-1)
            m['ceiling']=0
            m.loc[(m['low']==round(m['yesterdayClose']*1.1,2)),'ceiling']=1
            m['ceilingInNext5m']=m['ceiling'].shift(-5).rolling(5).max()
            m['maxLossInNext5m']=round((m['low'].shift(-5).rolling(5).min()-m['open'])/m['open']-1,2)
            m['ceilingInNext10m']=m['ceiling'].shift(-10).rolling(10).max()
            m['maxLossInNext10m']=round((m['low'].shift(-10).rolling(10).min()-m['open'])/m['open']-1,2)
            m[m['time']>'1450']['ceilingInNext5m','maxLossInNext5m','ceilingInNext10m','maxLossInNext10m']=None
            mselect=m[(m['increaseInDay']>0.07) & (m['increaseInDay']<0.08)]
            mselect=mselect.dropna(axis=0,how='any')
            self.__allMinute=self.__allMinute.append(mselect)
            pass
        pass
    #----------------------------------------------------------------------
    def __dataSelectOneByOne(self,code):
        m=self.__myMinute.getDataByDate(code,self.startDate,self.endDate)
        d=self.__myDaily.getDataByDate(code,self.startDate,self.endDate)
        days=list(d[d['high']>d['preClose']*1.05]['date'])
        m=m.loc[m['date'].isin(days),:]
        self.__dailyData[code]=d
        self.__minuteData[code]=m
        pass
    #----------------------------------------------------------------------
    def reverse(self,startDate,endDate):
        self.startDate=startDate
        self.endDate=endDate
        self.tradeDays=TradedayDataProcess.getTradedays(startDate,endDate)
        self.__dataPrepared()
        store = pd.HDFStore(self.__localFileStr,'a')
        store.append('ceiling',self.__allMinute,append=False,format="table",data_columns=['code', 'date', 'time', 'open', 'high', 'low', 'close', 'volume',
       'amount', 'increase5m', 'increase1m', 'yesterdayClose',
       'ceilingYesterday', 'ceilingYesterday2', 'ceilingIn5Days',
       'increaseInDay', 'ceiling', 'ceilingInNext5m', 'ceilingInNext10m','maxLossInNext5m','maxLossInNext10m'])
        store.close()


########################################################################
