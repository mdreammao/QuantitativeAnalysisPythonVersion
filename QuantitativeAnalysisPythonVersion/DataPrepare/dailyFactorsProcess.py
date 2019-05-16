from Config.myConstant import *
from Config.myConfig import *
from Utility.ComputeUtility import *
from Utility.HDF5Utility import *
from Utility.JobLibUtility import *
from DataAccess.KLineDataProcess import *
from DataAccess.TradedayDataProcess import *
from DataAccess.StockSharesProcess import *
from DataAccess.StockIPOInfoProcess import *
from DataAccess.IndustryClassification import *
import numpy as np
import datetime 

########################################################################
class dailyFactorsProcess(object):
    """计算日线上的衍生因子，按照文件形式存放"""
    #----------------------------------------------------------------------
    def __init__(self):
        self.localFileFolder=os.path.join(LocalFileAddress,'dailyFactors')
        #self.tradedays=TradedayDataProcess.getAllTradedays()
        today=datetime.datetime.now().strftime("%Y%m%d")
        #取最近100天的数据
        self.endDate=TradedayDataProcess.getPreviousTradeday(today)
        #self.startDate=TradedayDataProcess.getPreviousTradeday(today,100) 
        pass
    #----------------------------------------------------------------------
    def __updateStockDailyBasicData(self,code,startDate,endDate):
        marketData=KLineDataProcess('daily',True)
        mydata=marketData.getDataByDate(code,startDate,endDate)
        return mydata
        pass
    #----------------------------------------------------------------------
    """获取单个股票指定因子的特征"""
    def getSingleStockDailyFactors(self,code,factorList,startDate,endDate):
        startDate=str(startDate)
        endDate=str(endDate)
        tradedays=TradedayDataProcess.getTradedays(startDate,endDate)
        mydata=pd.DataFrame(tradedays,columns=['date'])
        mydata['code']=code
        for factor in factorList:
            fileName=code.replace('.','_')+".h5"
            factorFilePath=os.path.join(self.localFileFolder,factor,fileName)
            exists=os.path.isfile(factorFilePath)
            if exists==False:
                print(f'error! {code} has no factor {factor}!!')
                pass
            else:
                store = pd.HDFStore(path=factorFilePath,mode='a',complib='blosc:zstd',append=True,complevel=9)
                factorData=store['factors']
                factorData=factorData[(factorData['date']>=startDate) & (factorData['date']<=endDate)]
                store.close()
                mydata=pd.merge(mydata,factorData,how='left',left_on='date',right_on='date')
                pass
        return mydata
    #----------------------------------------------------------------------
    """获取多个股票指定因子的特征"""
    def getMultipleStockDailyFactors(self,codeList,factorList,startDate,endDate):
        startDate=str(startDate)
        endDate=str(endDate)
        allData=pd.DataFrame()
        for code in codeList:
            mydata= self.getSingleStockDailyFactors(code,factorList,startDate,endDate)
            allData=allData.append(mydata)
            pass
        return allData
        pass
    #----------------------------------------------------------------------
    """并行更新股票因子的特征"""
    def parallelizationUpdateFactors(self,codeList,factorList):
        mydata=JobLibUtility.useJobLibToUpdateFacotrs(self.updateStockDailyFactors,codeList,MYGROUPS,factorList)
        pass
    #----------------------------------------------------------------------
    """给定原始数据，计算指定股票指定因子的特征"""
    def updateStockDailyFactors(self,codeList,factorList):
        for code in codeList:
            logger.info(f'{code} factor update start!')
            #获取数据
            IPOInfo=StockIPOInfoProcess.getStockIPOInfoByCode(code)
            listDate=IPOInfo['listDate'].iloc[0]
            delistDate=IPOInfo['delistDate'].iloc[0]
            #mydata=self.__updateStockDailyBasicData(code)
            for factor in factorList:
                fileName=code.replace('.','_')+".h5"
                factorFilePath=os.path.join(self.localFileFolder,factor,fileName)
                path=os.path.join(self.localFileFolder,factor)
                HDF5Utility.pathCreate(path)
                exists=os.path.isfile(factorFilePath)
                if exists==False:
                    startDate=listDate
                    endDate=self.endDate
                    lastDate=EMPTY_STRING
                    pass
                else:
                    store = pd.HDFStore(path=factorFilePath,mode='a',complib='blosc:zstd',append=True,complevel=9)
                    existsDate=store.select('date')
                    lastDate=existsDate.max()
                    startDate=lastDate
                    endDate=self.endDate
                    store.close()
                    if startDate>=endDate:
                        continue
                pass
                startDate=TradedayDataProcess.getPreviousTradeday(startDate,100)
                if startDate<listDate:
                    startDate=listDate
                    pass
                if endDate>delistDate:
                    endDate=delistDate
                    pass
                #marketData=self.__updateStockDailyBasicData(code,startDate,endDate)
                dailyData=KLineDataProcess('daily')
                marketData=dailyData.getDataByDate(code,startDate,endDate)
                endDate=marketData['date'].max() #因子日期统一计算到日线数据最后一天
                mydata=marketData
                #tradedays=TradedayDataProcess.getTradedays(startDate,endDate)
                #mydata=pd.DataFrame(tradedays,columns=['date'])
                #mydata=pd.merge(mydata,marketData,how='left',left_on='date',right_on='date')
                if factor=='closeStd':
                    myReturnFun=ComputeUtility.computeReturn
                    myStdFun=ComputeUtility.computeStandardDeviation
                    myTSRank=ComputeUtility.computeTimeSeriesRank
                    mydata=self.__computeStockDailyFactor(mydata,myReturnFun,['close','preClose','return'])
                    mydata.loc[mydata['status']=='停牌','return']=np.nan
                    mydata.loc[mydata['date']==listDate,'return']=np.nan
                    mydata=self.__computeStockDailyFactor(mydata,myStdFun,['return','closeStd20',20,0.8])
                    mydata=self.__computeStockDailyFactor(mydata,myTSRank,['closeStd20','ts_closeStd20',50,0.4])
                    mycolumns=['date','return','closeStd20','ts_closeStd20']
                    mydata=mydata[mycolumns]
                elif factor=='industry':
                    mydata.set_index('date',drop=True,inplace=True)
                    myindustry=IndustryClassification.getIndustryByCode(code,startDate,endDate)
                    mydata['industry']=myindustry['industry']
                    mydata['industryName']=myindustry['name']
                    mydata.reset_index(drop=False,inplace=True)
                    mycolumns=['date','industry','industryName']
                    mydata=mydata[mycolumns]
                    pass
                elif factor=='index':
                    myindex=IndexComponentDataProcess()
                    myIndexBelongs50=myindex.getStockBelongs(code,SSE50,startDate,endDate)
                    myIndexBelongs300=myindex.getStockBelongs(code,HS300,startDate,endDate)
                    myIndexBelongs500=myindex.getStockBelongs(code,CSI500,startDate,endDate)
                    myStockWeightOf50=IndexComponentDataProcess.getStockPropertyInIndex(code,SSE50,startDate,endDate)
                    myStockWeightOf300=IndexComponentDataProcess.getStockPropertyInIndex(code,HS300,startDate,endDate)
                    myStockWeightOf500=IndexComponentDataProcess.getStockPropertyInIndex(code,CSI500,startDate,endDate)
                    mydata.set_index('date',drop=True,inplace=True)
                    mydata['is50']=myIndexBelongs50['exists']
                    mydata['is300']=myIndexBelongs300['exists']
                    mydata['is500']=myIndexBelongs500['exists']
                    mydata['weight50']=myStockWeightOf50['weight']
                    mydata['weight300']=myStockWeightOf300['weight']
                    mydata['weight500']=myStockWeightOf500['weight']
                    mydata.reset_index(drop=False,inplace=True)
                    mycolumns=['date','is50','is300','is500','weight50','weight300','weight500']
                    mydata=mydata[mycolumns]
                    mydata[['is50','is300','is500']]=mydata[['is50','is300','is500']].astype('int64')
                    pass
                elif factor=='marketValue':
                    mydata.set_index('date',drop=True,inplace=True)
                    myDailyDerivative=KLineDataProcess('dailyDerivative')
                    mydataDerivative=myDailyDerivative.getDataByDate(code,startDate,endDate)
                    mydataDerivative.set_index('date',inplace=True)
                    mydata['freeShares']=mydataDerivative['freeShares']
                    mydata['freeMarketValue']=mydataDerivative['freeMarketValue']
                    mydata.reset_index(drop=False,inplace=True)
                    mycolumns=['date','freeShares','freeMarketValue']
                    mydata=mydata[mycolumns]
                store = pd.HDFStore(path=factorFilePath,mode='a',complib='blosc:zstd',append=True,complevel=9)
                if lastDate==EMPTY_STRING:
                    #mydata=mydata[(mydata['date']<'20181231')]
                    mydate=mydata['date']
                    pass
                else:
                    mydata=mydata[(mydata['date']>lastDate)]
                    mydate=mydata['date']
                    pass
                if mydata.empty==False:
                    store.append('date',mydate,append=True,format="table",data_columns=['date'],complevel=9)
                    store.append('factors',mydata,append=True,format="table",data_columns=mydata.columns,complevel=9)
                    pass
                store.close()
            pass
        pass
    #----------------------------------------------------------------------
    """给定原始数据，计算指定股票指定因子的特征"""
    def __computeStockDailyFactor(self,mydata,myfun,parameters=[]):
        result=myfun(mydata,parameters)
        return result
        pass
    #----------------------------------------------------------------------
    def __getStockDailyFactor(self,code,facotr,startDate,endDate):
        pass
########################################################################