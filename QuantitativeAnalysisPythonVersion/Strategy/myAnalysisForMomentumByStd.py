from DataAccess.IndexComponentDataProcess import *
from DataAccess.KLineDataProcess import *
from DataAccess.IndustryClassification import *
from DataAccess.TradedayDataProcess import *
from Utility.ReturnAnalysis import *
from Config.myConstant import *
from Config.myConfig import *
import copy

########################################################################
class myAnalysisForMomentumByStd(object):
    """股票异动,专注股票大涨之后的回调"""
    #----------------------------------------------------------------------
    def __init__(self):
        self.__localFileStrResult=LocalFileAddress+"\\intermediateResult\\stdMomentumResult.h5"
        self.__localFileStrResultAll=LocalFileAddress+"\\result\\stdMomentumResult.h5"
        pass
    def __detailAnalysis(self,mydata,startDate,endDate):
        address=LocalFileAddress+"\\intermediateResult\\momentum"
        #多空
        long=mydata[mydata['position']==1]
        short=mydata[mydata['position']==-1]
        
        #指数成分
        my50=mydata[mydata['is50']==1]
        my300=mydata[mydata['is300']==1]
        my500=mydata[mydata['is500']==1]
        others=mydata[(mydata['is50']==0) & (mydata['is300']==0) &(mydata['is500']==0)]
        #行业
        myindustry=self.analysisIndustry(mydata,address)
        print(myindustry)
        #时间
        #波动率
        #波动率rank
        
        
        
        allAnswer=ReturnAnalysis.getBasicInfo(mydata['return'])
        longAnswer=ReturnAnalysis.getBasicInfo(long['return'])
        shortAnswer=ReturnAnalysis.getBasicInfo(short['return'])
        my50Answer=ReturnAnalysis.getBasicInfo(my50['return'])
        my300Answer=ReturnAnalysis.getBasicInfo(my300['return'])
        my500Answer=ReturnAnalysis.getBasicInfo(my500['return'])
        othersAnswer=ReturnAnalysis.getBasicInfo(others['return'])
        '''
        self.__analysisByTradedays(mydata,startDate,endDate,address,'all')
        self.__analysisByTradedays(long,startDate,endDate,address,'long')
        self.__analysisByTradedays(short,startDate,endDate,address,'short')
        self.__analysisByTradedays(my50,startDate,endDate,address,'50')
        self.__analysisByTradedays(my300,startDate,endDate,address,'300')
        self.__analysisByTradedays(my500,startDate,endDate,address,'500')
        self.__analysisByTradedays(others,startDate,endDate,address,'others')
        '''
        
        ReturnAnalysis.getHist(mydata['return'],address,'all')
        ReturnAnalysis.getHist(long['return'],address,'long')
        ReturnAnalysis.getHist(short['return'],address,'short')
        ReturnAnalysis.getHist(my50['return'],address,'50')
        ReturnAnalysis.getHist(my300['return'],address,'300')
        ReturnAnalysis.getHist(my500['return'],address,'500')
        ReturnAnalysis.getHist(others['return'],address,'others')
        
        df=pd.DataFrame(allAnswer,index=['all'])
        df=df.append(pd.DataFrame(longAnswer,index=['long']))
        df=df.append(pd.DataFrame(shortAnswer,index=['short']))
        df=df.append(pd.DataFrame(my50Answer,index=['50']))
        df=df.append(pd.DataFrame(my300Answer,index=['300']))
        df=df.append(pd.DataFrame(my500Answer,index=['500']))
        df=df.append(pd.DataFrame(othersAnswer,index=['others']))
        print(df)
        pass
   #----------------------------------------------------------------------
    def __analysisByTradedays(self,mydata,startDate,endDate,address,nameStr=EMPTY_STRING):
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
                myfirst=todayData.sort_values('time').head(10)
                n=myfirst.shape[0]
                cash=cash+cashUnit*n*(myfirst['return'].mean()-0.004)
                pass
            netvalueList.append(cash/startCash)
        ReturnAnalysis.getNetValue(tradeDays,netvalueList,address,nameStr)
        pass
   #----------------------------------------------------------------------
    def analysisIndustry(self,mydata,address):
        industry=IndustryClassification.getIndustryClassification()
        industry['industry']=industry['industry'].astype('int32')
        industry['name']=industry['name'].astype('str')
        result=[]
        for index,row in industry.iterrows():
            industry0=row[1]
            name0=row[2]
            mydata0=mydata[mydata['industry']==industry0]
            answer=ReturnAnalysis.getBasicDescribe(mydata0['return'])
            answer=round(answer,4)
            result0=[industry0,name0,answer['count'],answer['mean'],answer['std'],answer['min'],answer['25%'],answer['50%'],answer['75%'],answer['max']]
            result.append(result0)
        result=pd.DataFrame(data=result,columns=['industry','name','count','mean','std','min','25%','50%','75%','max'])
        ReturnAnalysis.getBar(result['name'],result['mean'],'industry','mean',address,'industry')
        return result 
        pass
    #----------------------------------------------------------------------
    def analysis(self,startDate,endDate):
        startDate=str(startDate)
        endDate=str(endDate)
        tradeDays=TradedayDataProcess.getTradedays(startDate,endDate)
        mydata=pd.DataFrame()
        if os.path.exists(self.__localFileStrResultAll):
            store=pd.HDFStore(self.__localFileStrResultAll,'a')
            mydata=store.get("all")
            store.close()
        else:
            store = pd.HDFStore(self.__localFileStrResult,'a')
            keys=store.keys()
            num=0
            for code in keys:
                mycode=code.lstrip("/")
                mydata=mydata.append(store.get(mycode))
                num=num+1
                #print(num)
            store.close()
            store=pd.HDFStore(self.__localFileStrResultAll,'a')
            store.put("all",mydata,append=False,format='table')
            store.close()

        mydata=mydata[['code','date', 'time','closeDate', 'closeTime', 'feeRate', 'return','increaseInDay', 'closeStd20','amount',
       'increase5m', 'increase1m', 'industry', 'is50',
       'is300', 'is500', 'ts_rank_closeStd20',
       'rankMarketValue', 'position',  'open','closePrice','canBuy','canSell','canBuyPrice','canSellPrice'
       ]]
        mydata=mydata[(mydata['closePrice']>0) & (mydata['increaseInDay']>-0.2) & (mydata['increaseInDay']<0.2)]
        #mydata=mydata[((mydata['increase5m']<-1.5*mydata['closeStd20']) & (mydata['position']==-1)) |((mydata['increase5m']>1.5*mydata['closeStd20']) & (mydata['position']==1))]
        self.__detailAnalysis(mydata,startDate,endDate)

        print(mydata.shape)

     


########################################################################
