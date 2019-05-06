from Config.myConstant import *
from Config.myConfig import *
from DataAccess.IndexComponentDataProcess import *
from DataAccess.KLineDataProcess import *
from DataAccess.TradedayDataProcess import *
from DataAccess.StockSharesProcess import * 
from DataAccess.IndustryClassification import *
import time
import numpy as np
import os


########################################################################
class dailyKLineDataPrepared(object):
    """description of class"""
#----------------------------------------------------------------------
    def __init__(self):
        self.key='factors'
        self.keyWithRank='factorsWithRank'
        #self.localFileStr=LocalFileAddress+"\\{0}\\{1}.h5".format('dailyFactors',self.key)
        #self.localFileStrWithRank=LocalFileAddress+"\\{0}\\{1}.h5".format('dailyFactors',self.keyWithRank)
        self.localFileStr=os.path.join(LocalFileAddress,'dailyFactors',self.key+'.h5')
        self.localFileStrWithRank=os.path.join(LocalFileAddress,'dailyFactors',self.keyWithRank+'.h5')
        pass
#----------------------------------------------------------------------
    def getStockDailyFeatureData(self,stockCodes,startDate,endDate):
        myDaily=KLineDataProcess('daily',True)
        myDailyDerivative=KLineDataProcess('dailyDerivative',True)
        myindex=IndexComponentDataProcess(True)
        exists=os.path.isfile(self.localFileStr)
        if exists==True:
            store = pd.HDFStore(self.localFileStr,'a')
            allData=store.select(self.key)
            allData.reset_index(inplace=True,drop=False)
            existsCodes=list(allData['code'].drop_duplicates())
            store.close()
        else:
            allData=pd.DataFrame()
            existsCodes=[]
        num=0
        for code in stockCodes:
            num=num+1
            print("{0}({1} of {2}) start!".format(code,num,len(stockCodes)))
            if code in existsCodes:
                stockNow=allData[allData['code']==code]
                latestDate=stockNow['date'].max()
                if endDate<=latestDate:
                    continue
                pass
            startNow=max(startDate,TradedayDataProcess.getNextTradeday(latestDate))
            if startNow>endDate:
                continue
                pass
            mydata=myDaily.getDataByDate(code,startNow,endDate)
            mydata.set_index('date',drop=True,inplace=True)
            myindustry=IndustryClassification.getIndustryByCode(code,startNow,endDate)
            mydata['industry']=myindustry['industry']
            mydata['industryName']=myindustry['name']
            myIndexBelongs50=myindex.getStockBelongs(code,SSE50,startNow,endDate)
            myIndexBelongs300=myindex.getStockBelongs(code,HS300,startNow,endDate)
            myIndexBelongs500=myindex.getStockBelongs(code,CSI500,startNow,endDate)
            mydata['is50']=myIndexBelongs50['exists']
            mydata['is300']=myIndexBelongs300['exists']
            mydata['is500']=myIndexBelongs500['exists']
            mydataDerivative=myDailyDerivative.getDataByDate(code,startNow,endDate)
            mydataDerivative.set_index('date',inplace=True)
            mydata['freeShares']=mydataDerivative['freeShares']
            mydata['freeMarketValue']=mydataDerivative['freeMarketValue']
            mydata['return']=(mydata['close']-mydata['preClose'])/mydata['preClose']
            #昨日计算出的标准差给今日用，防止用到未来数据
            mydata['closeStd20']=mydata['return'].shift(-1).rolling(20,min_periods=17).std()
            mydata['ts_rank_closeStd20']=mydata['closeStd20'].rolling(50,min_periods=20).apply((lambda x:pd.Series(x).rank().iloc[-1]/len(x)),raw=True)
            mydata.reset_index(inplace=True)
            allData=allData.append(mydata)
            pass
        allData=allData.set_index(['date','code'])
        store = pd.HDFStore(self.localFileStr,'a')
        store.append(self.key,allData,append=True,format="table")
        store.close()
        unstack=allData.unstack()
        rankMv=unstack['freeMarketValue'].rank(axis=1)
        mvMax=rankMv.max(axis=1)
        rankMv=rankMv.iloc[:,:].div(mvMax,axis=0)
        #做rankMarketValue的操作
        allData['rankMarketValue']=rankMv.stack()
        store = pd.HDFStore(self.localFileStrWithRank,'a')
        store.append(self.keyWithRank,allData,append=False,format="table")
        store.close()
########################################################################


