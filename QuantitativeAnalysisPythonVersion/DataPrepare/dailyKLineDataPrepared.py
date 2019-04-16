from Config.myConstant import *
from Config.myConfig import *
from DataAccess.IndexComponentDataProcess import *
from DataAccess.KLineDataProcess import *
from DataAccess.TradedayDataProcess import *
from DataAccess.StockSharesProcess import * 
from DataAccess.IndustryClassification import *
import time
import numpy as np


########################################################################
class dailyKLineDataPrepared(object):
    """description of class"""
#----------------------------------------------------------------------
    def __init__(self):
        self.key='factors'
        self.localFileStr=LocalFileAddress+"\\{0}\\{1}.h5".format('dailyFactors',self.key)
        pass
#----------------------------------------------------------------------
    def getStockDailyFeatureData(self,startDate,endDate):
        myindex=IndexComponentDataProcess(True)
        index500=myindex.getCSI500DataByDate(startDate,endDate)
        index300=myindex.getHS300DataByDate(startDate,endDate)
        index50=myindex.getSSE50DataByDate(endDate,endDate)
        stockCodes=list(pd.concat([index50],ignore_index=True)['code'].drop_duplicates())
        #stockCodes=list({'002138.SZ','600000.SH','600958.SH'})
        myDaily=KLineDataProcess('daily',True)
        num=0
        allData=pd.DataFrame()
        for code in stockCodes:
            print(datetime.datetime.now())
            num=num+1
            mydata=myDaily.getDataByDate(code,startDate,endDate)
            myindustry=IndustryClassification.getIndustryByCode(code,startDate,endDate)
            mydata['industry']=myindustry['industry']
            mydata['industryName']=myindustry['name']
            myIndexBelongs50=myindex.getStockBelongs(code,SSE50,startDate,endDate)
            myIndexBelongs300=myindex.getStockBelongs(code,HS300,startDate,endDate)
            myIndexBelongs500=myindex.getStockBelongs(code,CSI500,startDate,endDate)
            mydata['is50']=myIndexBelongs50['exists']
            mydata['is300']=myIndexBelongs300['exists']
            mydata['is500']=myIndexBelongs500['exists']
            mydata['ceiling']=0
            mydata['ceilingYesterday']=0
            mydata['ceilingYesterday2']=0
            mydata['ceilingIn5Days']=0
            mydata.loc[(mydata['close']==round(mydata['preClose']*1.1,2)),'ceiling']=1
            mydata.loc[(mydata['ceiling'].shift(1)==1),'ceilingYesterday']=1
            mydata.loc[((mydata['ceiling'].shift(1)==1) & (mydata['ceiling'].shift(2)==1)),'ceilingYesterday2']=1
            mydata['ceilingIn5Days']=mydata['ceilingYesterday'].rolling(5).sum()
            mydata.set_index('date',inplace=True)
            mv=StockSharesProcess.getStockShares(code,startDate,endDate)
            mv.set_index('date',inplace=True)
            mydata['freeShares']=mv['freeShares']
            mydata['freeMarketValue']=mydata['freeShares']*mydata['preClose']
            mydata['return']=(mydata['close']-mydata['preClose'])/mydata['preClose']
            mydata['closeStd20']=mydata['return'].shift(-1).rolling(20,min_periods=17).std()
            mydata['ts_rank_closeStd20']=mydata['closeStd20'].rolling(50,min_periods=20).apply((lambda x:pd.Series(x).rank().iloc[-1]/len(x)),raw=True)
            mydata.reset_index(inplace=True)
            allData=allData.append(mydata)
            print("{0}({1} of (2)) complete!".format(code,num,len(stockCodes)))
        allData=allData.set_index(['date','code'])
        unstack=allData.unstack()
        rankMv=unstack['freeMarketValue'].rank(axis=1)
        mvMax=rankMv.max(axis=1)
        rankMv=rankMv.iloc[:,:].div(mvMax,axis=0)
        allData['rankMarketValue']=rankMv.stack()
        store = pd.HDFStore(self.localFileStr,'a')
        store.append(self.key,allData,append=False,format="table")
        store.close()
########################################################################


