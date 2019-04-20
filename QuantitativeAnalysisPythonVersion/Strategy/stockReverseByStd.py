from DataAccess.IndexComponentDataProcess import *
from DataAccess.KLineDataProcess import *
from DataAccess.TradedayDataProcess import *
from Config.myConstant import *
from Config.myConfig import *
import numpy as np
import numba

########################################################################
class stockReverseByStd(object):
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
            print("{0}({1} of {2}) start!".format(str(code),num,len(mylist)))
            #print(datetime.datetime.now())
            if ('/'+code) in oldKeys:
                #continue
                pass
            m=myMinute.getDataByDate(code,startDate,endDate)
            if len(m)==0:
                continue
            m=m[m['date'].isin(mytradedays)]
            m['vwap']=m['amount']/m['volume']
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
            m=m[m['status']!='N']
            m['canBuy']=0
            m['canSell']=0
            m['canBuyPrice']=None
            m['canSellPrice']=None
            m.loc[ (m['open']<round(1.097*m['yesterdayClose'],2)),'canBuy']=1
            m.loc[(m['open']>round(0.903*m['yesterdayClose'],2)),'canSell']=1
            m.loc[m['canBuy']==1,'canBuyPrice']=m.loc[m['canBuy']==1,'open']
            m['canBuyPrice']=m['canBuyPrice'].fillna(method='bfill')
            m.loc[m['canSell']==1,'canSellPrice']=m.loc[m['canSell']==1,'open']
            m['canSellPrice']=m['canSellPrice'].fillna(method='bfill')
            m['shortReturnNext20m']=round((m['canBuyPrice'].shift(-20)*m['adjFactor'].shift(-20)-m['open']*m['adjFactor'])/(m['open']*m['adjFactor']),4)
            m['longReturnNext20m']=round((m['canSellPrice'].shift(-20)*m['adjFactor'].shift(-20)-m['open']*m['adjFactor'])/(m['open']*m['adjFactor']),4)
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
        self.tradeDays=TradedayDataProcess.getTradedays(startDate,endDate)
        self.__dataPrepared(stockCodes,startDate,endDate)
        mylist=stockCodes
        exists=os.path.isfile(self.__localFileStrResult)
        if exists==True:
            f=h5py.File(self.__localFileStrResult,'r')
            myKeys=list(f.keys())
            f.close()
        else:
            myKeys=[]
        storeResult = pd.HDFStore(self.__localFileStrResult,'a')
        store = pd.HDFStore(self.__localFileStr,'a')

       # print(datetime.datetime.now())
        num=0
        for code in mylist:
            num=num+1
            print("{0}({1} of {2}) start!".format(code,num,len(mylist)))
            if code in myKeys:
                continue
                pass
            result=pd.DataFrame()
            mydata=store.select(code)
            mydata=mydata[(mydata['date']>=startDate) & (mydata['date']<=endDate)]
            mydata.reset_index(drop=False,inplace=True)
            mydata['signal']=0
            
            mydata.loc[((mydata['increaseInDay']>2*mydata['closeStd20']) & (mydata['increaseInDay']<0.095) & (mydata['time']<'1440')& (mydata['time']>'0935') & (mydata['volume']!=0)),'signal']=-1
            mydata.loc[((mydata['increaseInDay']<-2*mydata['closeStd20']) & (mydata['increaseInDay']>-0.095) &(mydata['time']<'1440')& (mydata['time']>'0935')& (mydata['volume']!=0)),'signal']=1
            short=mydata[(mydata['signal']==-1) ]
            long=mydata[(mydata['signal']==1)]
            result=result.append(short)
            result=result.append(long)
            #short=short[(short['increase5m']>short['closeStd20'])]
            #long=long[(long['increase5m']<-long['closeStd20'])]
            print(short['shortReturnNext20m'].mean())
            print(long['longReturnNext20m'].mean())
            storeResult.append(code,result,append=False,format="table")
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
        storeResult.close()
        store.close()
        print(datetime.datetime.now())
        pass
    #----------------------------------------------------------------------
    def reverseByJit(self,stockCodes,startDate,endDate,storeStr=EMPTY_STRING):
        #@numba.jit(nopython=True,parallel=True)
        def mytransaction(m,days,parameter):
            length=len(m)
            z=np.zeros((days,3),dtype=np.int64)
            z0=np.zeros(3,dtype=np.int64)
            position=0
            openPrice=0
            startIndex=0
            endIndex=0
            num=0
            for i in range(0,length):
                increaseInDay=m[i][2]
                closeStd=m[i][3]
                mytime=m[i][1]
                canbuy=m[i][6]
                canbuyPrice=m[i][8]
                cansell=m[i][7]
                cansellPrice=m[i][9]
                if position==0:
                    if (mytime>935) & (mytime<1440):
                        if ((increaseInDay>parameter*closeStd) & (cansell==1)):
                            position=-1
                            startIndex=i
                            openPrice=cansellPrice
                            z0[0]=startIndex
                            z0[2]=position
                        elif ((increaseInDay<-parameter*closeStd) & (canbuy==1)):
                            position=1
                            startIndex=i
                            openPrice=canbuyPrice
                            z0[0]=startIndex
                            z0[2]=position
                elif position==1:
                    if (mytime>=1455) | (i>=startIndex+60) | ((i>=startIndex+20) & (cansellPrice<openPrice)):
                        position=0
                        endIndex=i
                        z0[1]=endIndex
                        z[num]=z0
                        num=num+1
                        z0=np.zeros(3,dtype=np.int64)
                elif position==-1:
                    if (mytime>=1455) | (i>=startIndex+60) | ((i>=startIndex+20) & (cansellPrice>openPrice)):
                        position=0
                        endIndex=i
                        z0[1]=endIndex
                        z[num]=z0
                        num=num+1
                        z0=np.zeros(3,dtype=np.int64)
            result0=np.zeros(28)
            length=num
            result=np.zeros((length,28))
            for i in range(0,num):
                position=z[i][2]
                startIndex=z[i][0]
                endIndex=z[i][1]
                startData=m[startIndex]
                endData=m[endIndex]
                result0[0:22]=startData[0:22] #开仓时候的全部信息
                result0[22]=position
                result0[23]=endData[0] #平仓日期
                result0[24]=endData[1] #平仓时间
                if position==1:
                    result0[25]=endData[9] #平仓价格
                    fee=startData[8]*0.0001+endData[9]*0.0011
                    result0[26]=fee/startData[8]
                    myreturn=(endData[9]*endData[5]-startData[8]*startData[5])/(startData[8]*startData[5])-result0[26]
                    result0[27]=myreturn
                    pass
                elif position==-1:
                    result0[25]=endData[8] #平仓价格
                    fee=startData[9]*0.0011+endData[8]*0.0001
                    result0[26]=fee/startData[9]
                    myreturn=-(endData[8]*endData[5]-startData[9]*startData[5])/(startData[9]*startData[5])-result0[26]
                    result0[27]=myreturn
                    pass
                result[i]=result0
                result0=np.zeros(28)
            return result
        startDate=str(startDate)
        endDate=str(endDate)
        self.startDate=startDate
        self.endDate=endDate
        self.tradeDays=TradedayDataProcess.getTradedays(startDate,endDate)
        days=len(self.tradeDays)
        mylist=stockCodes
        if storeStr==EMPTY_STRING:
            storeStr=self.__localFileStrResult
        storeTMP=pd.HDFStore(storeStr,'a')
        store = pd.HDFStore(self.__localFileStr,'a')
        oldKeys=store.keys()
        exists=os.path.isfile(self.__localFileStrResult)
        if exists==True:
            storeResult=pd.HDFStore(self.__localFileStrResult,'a')
            resultOldKeys=storeResult.keys()
            storeResult.close()
        else:
            resultOldKeys={}
        num=0
        resultAll=pd.DataFrame()
        for code in mylist:
            num=num+1
            if ('/'+code) not in oldKeys:
                continue
                pass
            if ('/'+code) in resultOldKeys:
                oldResult=storeResult.get(code)
                resultAll=resultAll.append(oldResult)
                continue
                pass
            result=pd.DataFrame()
            print("{0}({1} of {2}) start!".format(code,num,len(mylist)))
            print(datetime.datetime.now())
            mydata=store.select(code)
            mydata=mydata[(mydata['date']>=startDate) & (mydata['date']<=endDate)]
            mydata.reset_index(drop=False,inplace=True)
            mycolumns=['date', 'time','increaseInDay','closeStd20','open','adjFactor','canBuy', 'canSell', 'canBuyPrice', 'canSellPrice', 'amount','increase5m','increase1m','yesterdayClose',     'industry','is50','is300','is500','freeShares', 'freeMarketValue','ts_rank_closeStd20','rankMarketValue']
            m=mydata[mycolumns]
            m['industry']=m['industry'].fillna(method='ffill')
            m.dropna(inplace=True,subset=['increase5m','increase1m','closeStd20','ts_rank_closeStd20'])
            m[['date','time','industry']]=m[['date','time','industry']].astype(np.int64)
            m=m.as_matrix()
            result=mytransaction(m,days,2.5)
            result=pd.DataFrame(data=result,columns=['date', 'time','increaseInDay','closeStd20','open','adjFactor','canBuy', 'canSell', 'canBuyPrice', 'canSellPrice', 'amount','increase5m','increase1m','yesterdayClose',     'industry','is50','is300','is500','freeShares', 'freeMarketValue','ts_rank_closeStd20','rankMarketValue','position','closeDate','closeTime','closePrice','feeRate','return'])
            result[['date', 'time','canBuy', 'canSell',     'industry','is50','is300','is500','position','closeDate','closeTime']]=result[['date', 'time','canBuy', 'canSell',     'industry','is50','is300','is500','position','closeDate','closeTime']].astype(int)
            result['code']=code
            #resultAll=resultAll.append(result)
            #storeResult.append(code,result,append=False,format="table")
            storeTMP.put(code,result,append=False,format="table")
        store.close()
        storeTMP.close()
        return self.__localFileStrResult
        #print(resultAll.shape)
        #print(resultAll['return'].mean())
        pass

########################################################################
