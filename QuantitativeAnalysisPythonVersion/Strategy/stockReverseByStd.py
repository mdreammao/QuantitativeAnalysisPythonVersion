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
        self.__localFileStrResult=LocalFileAddress+"\\intermediateResult\\stdFeatureResult.h5"
        self.__allMinute=pd.DataFrame()
        self.__key='factorsWithRank'
        self.__factorsAddress=LocalFileAddress+"\\{0}\\{1}.h5".format('dailyFactors',self.__key)
        pass
    #----------------------------------------------------------------------
    def __getStockList(self,startDate,endDate):
        myindex=IndexComponentDataProcess()
        index500=myindex.getCSI500DataByDate(startDate,endDate)
        index300=myindex.getHS300DataByDate(startDate,endDate)
        index50=myindex.getSSE50DataByDate(endDate,endDate)
        stockCodes=list(pd.concat([index500,index300,index50],ignore_index=True)['code'].drop_duplicates())
        return stockCodes
    #----------------------------------------------------------------------
    def __dataPrepared(self,stockCodes,startDate,endDate):
        mytradedays=TradedayDataProcess.getTradedays(startDate,endDate)
        mylist=stockCodes
        store = pd.HDFStore(self.__localFileStr,'a')
        store.append('stockCodes',pd.DataFrame(stockCodes,columns=['code']),append=False,format="table")
        store.close()
        self.__mylist=mylist
        myMinute=KLineDataProcess('minute',False)
        num=0
        store = pd.HDFStore(self.__factorsAddress,'a')
        allDailyData=store.select(self.__key,where=['date>="%s" and date<="%s"'%(startDate,endDate)])
        store.close()
        store = pd.HDFStore(self.__localFileStr,'a')
        oldKeys=store.keys()
        for code in mylist:
            num=num+1
            print("{0}({1} of {2}) start!".format(code,num,len(mylist)))
            #print(datetime.datetime.now())
            if ('/'+code) in oldKeys:
                #continue
                pass
            m=myMinute.getDataByDate(code,startDate,endDate)
            if len(m)==0:
                continue
            m=m[m['date'].isin(mytradedays)]
            m['vwap']=m['amount']/m['volume']
            #print(d[d['ceilingYesterday']>0][['date','close','preClose','ceilingIn5Days']])
            m.loc[(m['date']==m['date'].shift(5)),'increase5m']=(m['open']/m['open'].shift(5)-1)
            m.loc[(m['date']==m['date'].shift(1)),'increase1m']=(m['open']/m['open'].shift(1)-1)
            d=allDailyData.xs(code,level='code')
            dailyInfo=d.loc[m['date']]
            deleteColumns=['code','date','open','high','low','close','volume','amount','change','pctChange','vwap']
            mycolumns=[col for col in dailyInfo.columns if col not in deleteColumns]
            dailyInfo=dailyInfo[mycolumns]
            dailyInfo.rename(columns={'preClose':'yesterdayClose'}, inplace=True) 
            dailyInfo.index=m.index
            m=pd.concat([m,dailyInfo],axis=1)
            m['increaseInDay']=(m['open']/m['yesterdayClose']-1)
            #m['ceiling']=0
            #m.loc[(m['low']==round(m['yesterdayClose']*1.1,2)),'ceiling']=1
            #m['ceilingInNext5m']=m['ceiling'].shift(-5).rolling(5).max()
            #m['ceilingInNext10m']=m['ceiling'].shift(-10).rolling(10).max()
            #m['returnInNext5m']=round((m['open'].shift(-5)-m['open'])/m['open']-1,2)
            #m['returnInNext10m']=round((m['open'].shift(-10)-m['open'])/m['open']-1,2)
            m['timeStamp']=m['date']+m['time']
            mselect=m.set_index(['timeStamp','code'])
            #m.xs(m[(((m['open']<m['open'][0]) & (m['time']<='0800'))|(m['time']=='1500')) & (m['date']==m['date'][0])].index[0])
            #mselect=m[(m['open'].shift(1)<m['yesterdayClose']*(1+parameter*m['closeStd20'])) & (m['open']>m['yesterdayClose']*(1+parameter*m['closeStd20']))]
            #mselect=mselect.dropna(axis=0,how='any')
            store.append(code,mselect,append=False,format="table")
           # self.__allMinute=self.__allMinute.append(mselect)
            pass
        store.close()
        pass
    #----------------------------------------------------------------------
    def reverse(self,stockCodes,startDate,endDate):
        startDate=str(startDate)
        endDate=str(endDate)
        self.startDate=startDate
        self.endDate=endDate
        store = pd.HDFStore(self.__localFileStrResult,'a')
        store.append('stockCodes',pd.DataFrame(stockCodes,columns=['code']),append=False,format="table")
        store.close()
        self.tradeDays=TradedayDataProcess.getTradedays(startDate,endDate)
        self.__dataPrepared(stockCodes,startDate,endDate)
        mylist=stockCodes
        store = pd.HDFStore(self.__localFileStr,'a')
        result=pd.DataFrame()
       # print(datetime.datetime.now())
        num=0
        for code in mylist:
            num=num+1
            print("{0}({1} of {2}) start!".format(code,num,len(mylist)))
            mydata=store.select(code)
            mydata=mydata[(mydata['date']>=startDate) & (mydata['date']<=endDate)]
            mydata=mydata[mydata['status']!='N']
            #print(datetime.datetime.now())
            mydata['canBuy']=0
            mydata['canSell']=0
            mydata['canBuyPrice']=None
            mydata['canSellPrice']=None
            mydata.loc[ (mydata['open']<round(1.097*mydata['yesterdayClose'],2)) & (mydata['low']<round(1.097*mydata['yesterdayClose'],2)),'canBuy']=1
            mydata.loc[(mydata['open']>round(0.903*mydata['yesterdayClose'],2)) & (mydata['high']>round(0.903*mydata['yesterdayClose'],2)),'canSell']=1
            mydata.loc[mydata['canBuy']==1,'canBuyPrice']=mydata.loc[mydata['canBuy']==1,'open']
            mydata['canBuyPrice']=mydata['canBuyPrice'].fillna(method='bfill')
            mydata.loc[mydata['canSell']==1,'canSellPrice']=mydata.loc[mydata['canSell']==1,'open']
            mydata['canSellPrice']=mydata['canSellPrice'].fillna(method='bfill')
            mydata['shortReturnNext20m']=round((mydata['canBuyPrice'].shift(-20)*mydata['adjFactor'].shift(-20)-mydata['open']*mydata['adjFactor'])/(mydata['open']*mydata['adjFactor']),4)
            mydata['longReturnNext20m']=round((mydata['canSellPrice'].shift(-20)*mydata['adjFactor'].shift(-20)-mydata['open']*mydata['adjFactor'])/(mydata['open']*mydata['adjFactor']),4)
            mydata.reset_index(drop=False,inplace=True)
            mydata['signal']=0
            
            mydata.loc[((mydata['increaseInDay']>1*mydata['closeStd20']) & (mydata['increaseInDay']<0.095) & (mydata['time']<'1440')& (mydata['time']>'0935') & (mydata['volume']!=0)),'signal']=-1
            mydata.loc[((mydata['increaseInDay']<-1*mydata['closeStd20']) & (mydata['increaseInDay']>-0.095) &(mydata['time']<'1440')& (mydata['time']>'0935')& (mydata['volume']!=0)),'signal']=1
            '''
            mydata.loc[((mydata['increaseInDay']>0.07) & (mydata['increaseInDay']<0.08) & (mydata['time']<'1440')& (mydata['time']>'0935') & (mydata['volume']!=0)),'signal']=-1
            mydata.loc[((mydata['increaseInDay']<-0.07) & (mydata['increaseInDay']>-0.08) &(mydata['time']<'1440')& (mydata['time']>'0935')& (mydata['volume']!=0)),'signal']=1
            '''
            short=mydata[(mydata['signal']==-1) ]
            long=mydata[(mydata['signal']==1)]
            result=result.append(short)
            result=result.append(long)
            #short=short[(short['increase5m']>short['closeStd20'])]
            #long=long[(long['increase5m']<-long['closeStd20'])]
            print(short['shortReturnNext20m'].mean())
            print(long['longReturnNext20m'].mean())
            #maxlen=mydata.shape[0]
            #num=0
            '''
            for indexs, row in mydata2.iterrows():
                num=num+1
                now=mydata[indexs+20:min(indexs+60,maxlen)]
                startPrice=row['open']
                mydate=row['date']
                tmp=now[((now['date']==mydate) & (now['open']<startPrice)) |(now['time']=='1500')]
                if tmp.empty:
                    mydataShort.loc[indexs,'outprice']=now.loc[min(indexs+60,maxlen)]['canBuyPrice']
                else:
                    mydataShort.loc[indexs,'outprice']=tmp.iloc[0]['canBuyPrice']
                if num>=1000:
                    break
                #print(indexs)
            #mydata['tmp']=mydata.apply(lambda mm:mm['open']**mm['close'],axis=1)
            '''
            print(datetime.datetime.now())
        
        store.close()
        store = pd.HDFStore(self.__localFileStrResult,'a')
        store.append('result',result,append=False,format="table")
        store.close()
        print(datetime.datetime.now())
        pass


########################################################################
