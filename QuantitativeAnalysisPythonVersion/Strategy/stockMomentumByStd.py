from DataAccess.IndexComponentDataProcess import *
from DataAccess.KLineDataProcess import *
from DataAccess.TradedayDataProcess import *
from Utility.JobLibUtility import *
import warnings
from Config.myConstant import *
from Config.myConfig import *
import numpy as np
import numba


########################################################################
class stockMomentumByStd(object):
    """股票异动,专注股票大涨之后的回调"""
    #----------------------------------------------------------------------
    def __init__(self):
        self.__localFileStr=LocalFileAddress+"\\intermediateResult\\stdFeature.h5"
        self.__localFileStrResult=LocalFileAddress+"\\intermediateResult\\stdMomentumResult.h5"
        self.__localFileStrResultOneFile=LocalFileAddress+"\\result\\stdMomentumResult.h5"
        self.__allMinute=pd.DataFrame()
        self.__key='factorsWithRank'
        self.__factorsAddress=LocalFileAddress+"\\{0}\\{1}.h5".format('dailyFactors',self.__key)
        pass
    #----------------------------------------------------------------------
    def getStockList(self,startDate,endDate):
        
        fileStr=LocalFileAddress+"\\{0}\\{1}.h5".format('dailyFactors','stockCodes')
        store = pd.HDFStore(fileStr,'a')
        stockCodes=list(store.select('stockCodes')['code'])
        store.close()
        stockCodes.remove('601268.SH')
        return stockCodes
    #----------------------------------------------------------------------
    def parallelizationDataPrepared(self,startDate,endDate):
        stockCodes=self.getStockList(startDate,endDate)
        JobLibUtility.useJobLib(self.dataPrepared,stockCodes,80,startDate,endDate,self.__localFileStr)
        pass

    #----------------------------------------------------------------------
    def dataPrepared(self,stockCodes,startDate,endDate,recordFilePath):
        warnings.filterwarnings('ignore')
        mytradedays=TradedayDataProcess.getTradedays(startDate,endDate)
        mylist=stockCodes
        myMinute=KLineDataProcess('minute',False)
        num=0
        store = pd.HDFStore(self.__factorsAddress,'a')
        allDailyData=store.select(self.__key,where=['date>="%s" and date<="%s"'%(startDate,endDate)])
        store.close()
        store = pd.HDFStore(recordFilePath,'a')
        oldKeys=store.keys()
        for code in mylist:
            num=num+1
            print("{0}({1} of {2}) start!".format(str(code),num,len(mylist)))
            #print(datetime.datetime.now())
            if ('/'+code) in oldKeys:
                continue
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
            m=m[m['status']!='N']
            m['canBuy']=0
            m['canSell']=0
            m['canBuyPrice']=None
            m['canSellPrice']=None
            m.loc[ ((m['open']<round(1.097*m['yesterdayClose'],2)) & (m['open']>0)),'canBuy']=1
            m.loc[((m['open']>round(0.903*m['yesterdayClose'],2)) &(m['open']>0)),'canSell']=1
            m.loc[m['canBuy']==1,'canBuyPrice']=m.loc[m['canBuy']==1,'open']
            m.loc[m['canBuy']==1,'canBuyPriceAdj']=m.loc[m['canBuy']==1,'adjFactor']
            m['canBuyPrice']=m['canBuyPrice'].fillna(method='bfill')
            m['canBuyPriceAdj']=m['canBuyPriceAdj'].fillna(method='bfill')
            m.loc[m['canSell']==1,'canSellPrice']=m.loc[m['canSell']==1,'open']
            m.loc[m['canSell']==1,'canSellPriceAdj']=m.loc[m['canSell']==1,'adjFactor']
            m['canSellPrice']=m['canSellPrice'].fillna(method='bfill')
            m['canSellPriceAdj']=m['canSellPriceAdj'].fillna(method='bfill')
            m['timeStamp']=m['date']+m['time']
            #日内分钟信息
            #成交量在前20分钟的分位数
            m['ts_rank_volume']=m['volume'].rolling(20,min_periods=15).apply((lambda x:pd.Series(x).rank().iloc[-1]/len(x)),raw=True)
            #每分钟收益
            m['minuteReturn']=(m['close']-m['close'].shift(-1))/m['close'].shift(-1)
            #收益标准差
            mydata['minuteStd20']=mydata['minuteReturn'].shift(-1).rolling(20,min_periods=17).std()
            #收益标准差分位数
            m['ts_rank_minuteStd20']=m['minuteStd20'].rolling(20,min_periods=15).apply((lambda x:pd.Series(x).rank().iloc[-1]/len(x)),raw=True)

            mselect=m.set_index(['timeStamp','code'])
            store.append(code,mselect,append=False,format="table")
            pass
        store.close()
        pass
    #----------------------------------------------------------------------
    def parallelizationMomentum(self,startDate,endDate):
        stockCodes=self.getStockList(startDate,endDate)
        JobLibUtility.useJobLibStoreToOneFile(self.momentum,stockCodes,80,startDate,endDate, self.__localFileStrResultOneFile)
        pass
    #----------------------------------------------------------------------
    def momentum(self,stockCodes,startDate,endDate,storeStr=EMPTY_STRING):
        warnings.filterwarnings('ignore')
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
            maxProfit=0
            lastOpenDate=0
            for i in range(0,length-1):
                increaseInDay=m[i][2]
                closeStd=m[i][3]
                priceNow=m[i][4]
                mytime=m[i][1]
                canbuy=m[i][6]
                canbuyPrice=m[i][8]
                canbuyPriceNext=m[i+1][8]
                cansell=m[i][7]
                cansellPrice=m[i][9]
                canSellPriceNext=m[i+1][9]
                today=m[i][0]
                if position==0:
                    if (mytime>935) & (mytime<1440):
                        if ((today>lastOpenDate) &(increaseInDay>parameter*closeStd) &(increaseInDay<(parameter+0.2)*closeStd) & (canbuy==1)):
                            position=1
                            startIndex=i
                            openPrice=canbuyPriceNext
                            z0[0]=startIndex
                            z0[2]=position
                            maxProfit=0
                            lastOpenDate=today
                        elif ((today>lastOpenDate) &(increaseInDay<-parameter*closeStd) & (increaseInDay>-(parameter+0.2)*closeStd) &(cansell==1)):
                            position=-1
                            startIndex=i
                            openPrice=canSellPriceNext
                            z0[0]=startIndex
                            z0[2]=position
                            maxProfit=0
                            lastOpenDate=today
                elif position==1:
                    profit=(priceNow-openPrice)/openPrice
                    maxProfit=max(profit,maxProfit)
                    if (((mytime>=1455) & (profit<0.05)) | (profit<maxProfit-0.5*closeStd)):
                        position=0
                        endIndex=i
                        z0[1]=endIndex
                        z[num]=z0
                        num=num+1
                        z0=np.zeros(3,dtype=np.int64)
                elif position==-1:
                    profit=(-priceNow+openPrice)/openPrice
                    maxProfit=max(profit,maxProfit)
                    if (((mytime>=1455) & (profit<0.05)) | (profit<maxProfit-0.5*closeStd)):
                        position=0
                        endIndex=i
                        z0[1]=endIndex
                        z[num]=z0
                        num=num+1
                        z0=np.zeros(3,dtype=np.int64)
            result0=np.zeros(30)
            length=num
            result=np.zeros((length,30))
            for i in range(0,num):
                position=z[i][2]
                startIndex=z[i][0]
                endIndex=z[i][1]
                startData=m[startIndex+1]
                endData=m[endIndex+1]
                result0[0:22]=startData[0:22] #开仓时候的全部信息
                result0[28]=startData[22]#canbuyPriceAdj
                result0[29]=startData[23]#canSellPriceAdj
                result0[22]=position
                result0[23]=endData[0] #平仓日期
                result0[24]=endData[1] #平仓时间
                if position==1:
                    result0[25]=endData[9] #平仓价格
                    fee=startData[8]*0.0001+endData[9]*0.0011
                    result0[26]=fee/startData[8]
                    myreturn=(endData[9]* result0[29]-startData[8]*startData[5])/(startData[8]*startData[5])-result0[26]
                    result0[27]=myreturn
                    pass
                elif position==-1:
                    result0[25]=endData[8] #平仓价格
                    fee=startData[9]*0.0011+endData[8]*0.0001
                    result0[26]=fee/startData[9]
                    myreturn=-(endData[8]*result0[28]-startData[9]*startData[5])/(startData[9]*startData[5])-result0[26]
                    result0[27]=myreturn
                    pass
                result[i]=result0
                result0=np.zeros(30)
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
            mycolumns=['date', 'time','increaseInDay','closeStd20','open','adjFactor','canBuy', 'canSell', 'canBuyPrice', 'canSellPrice', 'amount','increase5m','increase1m','yesterdayClose',     'industry','is50','is300','is500','freeShares', 'freeMarketValue','ts_rank_closeStd20','rankMarketValue','canBuyPriceAdj','canSellPriceAdj']
            m=mydata[mycolumns]
            m['industry']=m['industry'].fillna(method='ffill')
            m['industry']=m['industry'].fillna(method='bfill')
            m.dropna(inplace=True,subset=['increase5m','increase1m','closeStd20','ts_rank_closeStd20'])
            m[['date','time','industry']]=m[['date','time','industry']].astype(np.int64)
            m=m.as_matrix()
            result=mytransaction(m,10*days,1)
            result=pd.DataFrame(data=result,columns=['date', 'time','increaseInDay','closeStd20','open','adjFactor','canBuy', 'canSell', 'canBuyPrice', 'canSellPrice', 'amount','increase5m','increase1m','yesterdayClose',     'industry','is50','is300','is500','freeShares', 'freeMarketValue','ts_rank_closeStd20','rankMarketValue','position','closeDate','closeTime','closePrice','feeRate','return','canBuyPriceAdj','canSellPriceAdj'])
            result[['date', 'time','canBuy', 'canSell',     'industry','is50','is300','is500','position','closeDate','closeTime']]=result[['date', 'time','canBuy', 'canSell','industry','is50','is300','is500','position','closeDate','closeTime']].astype(int)
            result['code']=code
            #resultAll=resultAll.append(result)
            #storeResult.append(code,result,append=False,format="table")
            storeTMP.put(code,result,append=False,format="table")
        store.close()
        storeTMP.close()
        if exists==True:
            storeResult.close()
        return self.__localFileStrResult
        #print(resultAll.shape)
        #print(resultAll['return'].mean())
        pass

########################################################################
