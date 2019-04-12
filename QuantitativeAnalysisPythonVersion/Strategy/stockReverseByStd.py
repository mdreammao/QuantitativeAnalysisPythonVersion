from DataAccess.IndexComponentDataProcess import *
from DataAccess.KLineDataProcess import *
from DataAccess.TradedayDataProcess import *
from Config.myConstant import *
from Config.myConfig import *


########################################################################
class stockReverseByStd(object):
    """股票异动,专注股票大涨之后的回调"""
    #----------------------------------------------------------------------
    def __init__(self):
        self.__localFileStr=LocalFileAddress+"\\intermediateResult\\stdFeature.h5"
        self.__allMinute=pd.DataFrame()
        self.__key='factors'
        self.__factorsAddress=LocalFileAddress+"\\{0}\\{1}.h5".format('dailyFactors',self.__key)
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
    def __dataPrepared(self,startDate,endDate):
        #mylist=self.__getStockList()
        mylist=list({'002138.SZ','600000.SH','600958.SH'})
        myMinute=KLineDataProcess('minute',False)
        num=0
        store = pd.HDFStore(self.__factorsAddress,'a')
        allDailyData=store.select(self.KLineLevel,where=['date>="%s" and date<="%s"'%(startDate,endDate)])
        store.close()
        parameter=2
        for code in mylist:
            num=num+1
            print("{0}({1} of {2}) start!".format(code,num,len(mylist)))
            print(datetime.datetime.now())
            m=myMinute.getDataByDate(code,startDate,endDate)
            m['vwap']=m['amount']/m['volume']
            #print(d[d['ceilingYesterday']>0][['date','close','preClose','ceilingIn5Days']])
            m.loc[(m['date']==m['date'].shift(5)),'increase5m']=(m['open']/m['open'].shift(5)-1)
            m.loc[(m['date']==m['date'].shift(1)),'increase1m']=(m['open']/m['open'].shift(1)-1)
            d=allDailyData['date',code]
            dailyInfo=d.loc[[m['date'],code],['preClose','ceilingYesterday','ceilingYesterday2','ceilingIn5Days','closeStd20','ts_rank_closeStd20','rankMarketValue']]
            dailyInfo.index=m.index
            m[['yesterdayClose','ceilingYesterday','ceilingYesterday2','ceilingIn5Days','closeStd20','ts_rank_closeStd20','rankMarketValue']]=dailyInfo
            m['increaseInDay']=(m['open']/m['yesterdayClose']-1)
            m['ceiling']=0
            m.loc[(m['low']==round(m['yesterdayClose']*1.1,2)),'ceiling']=1
            m['ceilingInNext5m']=m['ceiling'].shift(-5).rolling(5).max()
            m['ceilingInNext10m']=m['ceiling'].shift(-10).rolling(10).max()
            m['returnInNext5m']=round((m['open'].shift(-5)-m['open'])/m['open']-1,2)
            m['returnInNext10m']=round((m['open'].shift(-10)-m['open'])/m['open']-1,2)
            m[m['time']>'1450']['ceilingInNext5m','maxLossInNext5m','ceilingInNext10m','maxLossInNext10m']=None
            mselect=m[(m['open'].shift(1)<m['yesterdayClose']*(1+parameter*m['closeStd'])) & (m['open']>m['yesterdayClose']*(1+parameter*m['closeStd']))]
            #mselect=mselect.dropna(axis=0,how='any')
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
        store.append('minuteStd',self.__allMinute,append=False,format="table")
        store.close()


########################################################################
