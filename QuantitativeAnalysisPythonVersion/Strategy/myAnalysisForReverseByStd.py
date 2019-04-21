from DataAccess.IndexComponentDataProcess import *
from DataAccess.KLineDataProcess import *
from DataAccess.IndustryClassification import *
from DataAccess.TradedayDataProcess import *
from Utility.ReturnAnalysis import *
from Config.myConstant import *
from Config.myConfig import *
import copy

########################################################################
class myAnalysisForReverseByStd(object):
    """股票异动,专注股票大涨之后的回调"""
    #----------------------------------------------------------------------
    def __init__(self):
        self.__localFileStrResult=LocalFileAddress+"\\intermediateResult\\stdReverseResult.h5"
        pass
    def __detailAnalysis(self,mydata):
        address=LocalFileAddress+"\\intermediateResult"
        #多空
        long=mydata[mydata['position']==1]
        short=mydata[mydata['position']==-1]
        
        #指数成分
        my50=mydata[mydata['is50']==1]
        my300=mydata[mydata['is300']==1]
        my500=mydata[mydata['is500']==1]
        others=mydata[(mydata['is50']==0) & (mydata['is300']==0) &(mydata['is500']==0)]
        #行业
        myindustry=self.analysisIndustry(mydata)
        print(myindustry)
        #时间
        #波动率
        #波动率rank
        
        longAnswer=ReturnAnalysis.getBasicInfo(long['return'])
        shortAnswer=ReturnAnalysis.getBasicInfo(short['return'])
        my50Answer=ReturnAnalysis.getBasicInfo(my50['return'])
        my300Answer=ReturnAnalysis.getBasicInfo(my300['return'])
        my500Answer=ReturnAnalysis.getBasicInfo(my500['return'])
        othersAnswer=ReturnAnalysis.getBasicInfo(others['return'])
        print(longAnswer)
        
        ReturnAnalysis.getHist(long['return'],address,'long')
        print(shortAnswer)
        print(my50Answer)
        print(my300Answer)
        print(my500Answer)
        print(othersAnswer)
        pass
   #----------------------------------------------------------------------
    def analysisByTradedays(self,mydata):
        startDate=str(startDate)
        endDate=str(endDate)
        tradeDays=TradedayDataProcess.getTradedays(startDate,endDate)
        netvalueList=[]
        startCash=1000000
        cash=startCash
        cashUnit=startCash/10
        for days in tradeDays:
            today=int(days)
            todayData=mydata[mydata['date']==today]
            if len(todayData)==0:
                pass
            else:
                myfirst=todayData.sort_value('time').head(10)
                n=myfirst.shape[0]
                cash=cash+cashUnit*n*myfirst['return'].mean()
                pass
            netvalueList.append(cash)
        
        pass
   #----------------------------------------------------------------------
    def analysisIndustry(self,mydata):
        industry=IndustryClassification.getIndustryClassification()
        industry['industry']=industry['industry'].astype('int32')
        industry['name']=industry['name'].astype('str')
        result=[[] for i in range(33)]
        for index,row in industry.iterrows():
            industry0=row[1]
            name0=row[2]
            mydata0=mydata[mydata['industry']==industry0]
            answer=ReturnAnalysis.getBasicDescribe(mydata['return'])
            result0=[industry0,name0,answer['count'],answer['mean'],answer['std'],answer['min'],answer['25%'],answer['50%'],answer['75%'],answer['max']]
            print(result0)
            result[index]=copy.deepcopy(result0)
            #result[index].append(result0)
        result=pd.DataFrame(data=result,columns=['industry','name','count','mean','std','min','25%','50%','75%','max'])
        return result 
        pass
    #----------------------------------------------------------------------
    def analysis(self,startDate,endDate):
        startDate=str(startDate)
        endDate=str(endDate)
        tradeDays=TradedayDataProcess.getTradedays(startDate,endDate)
        
        mydata=pd.DataFrame()
        store = pd.HDFStore(self.__localFileStrResult,'a')
        keys=store.keys()
        for code in keys:
            mycode=code.lstrip("/")
            #print(mycode)
            mydata=mydata.append(store.get(mycode))
        store.close()
        mydata=mydata[['code','date', 'time','closeDate', 'closeTime', 'feeRate', 'return','increaseInDay', 'closeStd20','amount',
       'increase5m', 'increase1m', 'industry', 'is50',
       'is300', 'is500', 'ts_rank_closeStd20',
       'rankMarketValue', 'position',  'open','closePrice','canBuy','canSell','canBuyPrice','canSellPrice'
       ]]
        mydata=mydata[(mydata['closePrice']>0) & (mydata['increaseInDay']>-0.2) & (mydata['increaseInDay']<0.2)]
        mydata=mydata[((mydata['increase5m']>mydata['closeStd20']) & (mydata['position']==-1)) |((mydata['increase5m']<-mydata['closeStd20']) & (mydata['position']==1))]
        self.__detailAnalysis(mydata)

        print(mydata.shape)

     


########################################################################
