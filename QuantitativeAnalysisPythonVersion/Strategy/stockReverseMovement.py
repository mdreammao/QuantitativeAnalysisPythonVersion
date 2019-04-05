from DataAccess.IndexComponentDataProcess import *
from DataAccess.KLineDataProcess import *
from DataAccess.TradedayDataProcess import *


########################################################################
class stockReverseMovement(object):
    """股票异动,专注股票大涨之后的回调"""
    #----------------------------------------------------------------------
    def __init__(self):
        self.__myMinute=KLineDataProcess('minute',True)
        self.__myDaily=KLineDataProcess('daily',True)
        pass
    #----------------------------------------------------------------------
    def __getStockList(self):
        myindex=IndexComponentDataProcess()
        index50=myindex.getSSE50DataByDate(self.endDate,self.endDate)
        return list(index50['code'].drop_duplicates())
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
            d.loc[(d['close']==round(d['preClose']*1.1,2)),'ceiling']=1
            d.loc[(d['ceiling'].shift(1)==1),'ceilingYesterday']=1
            d.loc[((d['ceiling'].shift(1)==1) & (d['ceiling'].shift(2)==1)),'ceilingYesterday2']=1
            d['ceilingIn5Days']=d['ceilingYesterday']+d['ceilingYesterday'].shift(1)+d['ceilingYesterday'].shift(2)+d['ceilingYesterday'].shift(3)+d['ceilingYesterday'].shift(4)
            m.loc[(m['date']==m['date'].shift(5)),'increase5m']=(m['open']/m['open'].shift(5)-1)
            m.loc[(m['date']==m['date'].shift(1)),'increase1m']=(m['open']/m['open'].shift(1)-1)
            d=d.set_index('date')
            preClose=d.loc[m['date'],'preClose']
            preClose.index=m.index
            m['yesterdayClose']=preClose
            m['increaseInDay']=(m['open']/m['yesterdayClose']-1)
            m['ceilingIn5m']=(m['low']==round(m['yesterdayClose']*1.1,2))
            mselect=m[(m['increaseInDay']>0.08) & (m['increaseInDay']<0.085)]
            
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
        #self.tradeDays=TradedayDataProcess.getTradedays(startDate,endDate)
        self.__dataPrepared()


########################################################################
