from DataAccess.IndexComponentDataProcess import *
from DataAccess.KLineDataProcess import *
from DataAccess.TradedayDataProcess import *
from DataPrepare.dailyFactorsProcess import *
from Utility.HDF5Utility import *
from Utility.JobLibUtility import *
import warnings
from Config.myConstant import *
from Config.myConfig import *
import numpy as np
import os 
#import numba


########################################################################
class stockReverseByStd(object):
    """股票异动,专注股票大涨之后的回调"""
    #----------------------------------------------------------------------
    def __init__(self):
        #self.__localFileStr=LocalFileAddress+"\\intermediateResult\\stdFeature.h5"
        path=os.path.join(LocalFileAddress,'intermediateResult')
        HDF5Utility.pathCreate(path)
        self.__localFileStr=os.path.join(LocalFileAddress,'intermediateResult','stdFeature.h5')
        #self.__localFileStrResult=LocalFileAddress+"\\intermediateResult\\stdReverseResult.h5"
        self.__localFileStrResult=os.path.join(LocalFileAddress,'intermediateResult','stdReverseResult.h5')
        self.__allMinute=pd.DataFrame()
        self.__key='factorsWithRank'
        #self.__factorsAddress=LocalFileAddress+"\\{0}\\{1}.h5".format('dailyFactors',self.__key)
        self.__factorsAddress=os.path.join(LocalFileAddress,'dailyFactors',self.__key+'.h5')
        pass
    #----------------------------------------------------------------------
    def getStockList(self,startDate,endDate):
        #fileStr=LocalFileAddress+"\\{0}\\{1}.h5".format('dailyFactors','stockCodes')
        fileStr=os.path.join(LocalFileAddress,'stockCode.h5')
        store = pd.HDFStore(fileStr,'r')
        stockCodes=list(store['data'])
        store.close()
        return stockCodes
    #----------------------------------------------------------------------
    def parallelizationDataPrepared(self,startDate,endDate):
        stockCodes=self.getStockList(startDate,endDate)
        JobLibUtility.useJobLib(self.dataPrepared,stockCodes,80,startDate,endDate,self.__localFileStr)
        pass

    #----------------------------------------------------------------------
    def dataPrepared(self,stockCodes,startDate,endDate):
        warnings.filterwarnings('ignore')
        recordFilePath=self.__localFileStrResult
        mytradedays=TradedayDataProcess.getTradedays(startDate,endDate)
        mylist=stockCodes
        myMinute=dkl.KLineDataProcess('minute')
        num=0
        dailyFactor=dailyFactorsProcess()
        #factors=['closeStd','index','marketValue','industry']
        allDailyData=dailyFactor.getMultipleStockDailyFactors(mylist,startDate,endDate)
        #store = pd.HDFStore(self.__factorsAddress,'a')
        #allDailyData=store.select(self.__key,where=['date>="%s" and date<="%s"'%(startDate,endDate)])
        #store.close()
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
            m['timeStamp']=m['date']+m['tick']
            mselect=m.set_index(['timeStamp','code'])
            store.append(code,mselect,append=False,format="table")
            pass
        store.close()
        pass
    #----------------------------------------------------------------------
    def parallelizationReverse(self,startDate,endDate):
        stockCodes=self.getStockList(startDate,endDate)
        JobLibUtility.useJobLib(self.reverse,stockCodes,80,startDate,endDate,self.__localFileStrResult)
        pass
    #----------------------------------------------------------------------
    def reverse(self,stockCodes,startDate,endDate,storeStr=EMPTY_STRING):
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
            for i in range(0,length-1):
                increaseInDay=m[i][2]
                closeStd=m[i][3]
                priceNow=m[i][4]
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
                            #openPrice=cansellPrice
                            openPrice=m[i+1][9] # 成交价格向后平移一分钟
                            z0[0]=startIndex
                            z0[2]=position
                        elif ((increaseInDay<-parameter*closeStd) & (canbuy==1)):
                            position=1
                            startIndex=i
                            #openPrice=canbuyPrice
                            openPrice=m[i+1][8]# 成交价格向后平移一分钟
                            z0[0]=startIndex
                            z0[2]=position
                elif position==1:
                    if (mytime>=1455) | (i>=startIndex+60) | ((i>=startIndex+20) & (priceNow<openPrice)):
                        position=0
                        endIndex=i
                        z0[1]=endIndex
                        z[num]=z0
                        num=num+1
                        z0=np.zeros(3,dtype=np.int64)
                elif position==-1:
                    if (mytime>=1455) | (i>=startIndex+60) | ((i>=startIndex+20) & (priceNow>openPrice)):
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
                startIndex=z[i][0]+1#成交往后平移一分钟
                endIndex=z[i][1]+1#成交往后平移一分钟
                #startIndex=z[i][0]
                #endIndex=z[i][1]
                startData=m[startIndex] 
                endData=m[endIndex]
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
            mycolumns=['date', 'tick','increaseInDay','closeStd20','open','adjFactor','canBuy', 'canSell', 'canBuyPrice', 'canSellPrice', 'amount','increase5m','increase1m','yesterdayClose',     'industry','is50','is300','is500','freeShares', 'freeMarketValue','ts_rank_closeStd20','rankMarketValue','canBuyPriceAdj','canSellPriceAdj']
            m=mydata[mycolumns]
            m['industry']=m['industry'].fillna(method='ffill')
            m['industry']=m['industry'].fillna(method='bfill')
            m.dropna(inplace=True,subset=['increase5m','increase1m','closeStd20','ts_rank_closeStd20'])
            m[['date','tick','industry']]=m[['date','tick','industry']].astype(np.int64)
            m=m.as_matrix()
            result=mytransaction(m,days,2.5)
            result=pd.DataFrame(data=result,columns=['date', 'tick','increaseInDay','closeStd20','open','adjFactor','canBuy', 'canSell', 'canBuyPrice', 'canSellPrice', 'amount','increase5m','increase1m','yesterdayClose',     'industry','is50','is300','is500','freeShares', 'freeMarketValue','ts_rank_closeStd20','rankMarketValue','position','closeDate','closeTime','closePrice','feeRate','return','canBuyPriceAdj','canSellPriceAdj'])
            result[['date', 'tick','canBuy', 'canSell',     'industry','is50','is300','is500','position','closeDate','closeTime']]=result[['date', 'tick','canBuy', 'canSell','industry','is50','is300','is500','position','closeDate','closeTime']].astype(int)
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
