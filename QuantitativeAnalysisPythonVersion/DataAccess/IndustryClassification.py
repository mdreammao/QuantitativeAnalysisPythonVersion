import pymssql
import cx_Oracle as oracle
import pandas as pd
from Config.myConstant import *
from Config.myConfig import *
import datetime
import h5py
import os
from DataAccess.TradedayDataProcess import *
from dateutil.relativedelta import relativedelta

########################################################################
class IndustryClassification(object):
    """申万行业分类"""
    nowStr=datetime.datetime.now().strftime('%Y%m%d')
    localFileStr=LocalFileAddress+"\\industryClassification.h5"
    allIndustry=pd.DataFrame()
    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        pass
    #----------------------------------------------------------------------
    #返还一行dataframe格式，['code'股票代码,'industry'行业名称,'industryCode'行业代码,'name'行业名称,'industryName1'1级行业名称,'industryName2'2级行业名称,'industryName3'3级行业名称,'entry'股票进入行业日期,'remove'股票退出行业日期]
    @classmethod 
    def getIndustryByCodeDaily(self,code,mydate):
        code=code.upper()
        mydate=str(mydate)
        if len(IndustryClassification.allIndustry)==0:
            IndustryClassification.allIndustry=IndustryClassification.__getIndustryFromLocalFile()
        else:
            pass
        mydata=IndustryClassification.allIndustry
        mydata=mydata[mydata['code']==code]
        if len(mydata)==1:
            select=mydata
        else:
            select=mydata[(mydata['entry']<=mydate) & (mydata['remove']>=mydate)]
        select.set_index('date',drop=True,inplace=True)
        return select
    #----------------------------------------------------------------------
    #返还dataframe格式，['date'日期，'code'股票代码,'industry','name'行业名称]
    @classmethod 
    def getIndustryByCode(self,code,startDate,endDate):
        code=code.upper()
        startDate=str(startDate)
        endDate=str(endDate)
        if len(IndustryClassification.allIndustry)==0:
            IndustryClassification.allIndustry=IndustryClassification.__getIndustryFromLocalFile()
        else:
            pass
        mydata=IndustryClassification.allIndustry
        tradedays=TradedayDataProcess.getTradedays(startDate,endDate)
        dataWithIndex=pd.DataFrame(tradedays,columns=['date'])
        #dataWithIndex.set_index(['date'],inplace=True)
        mydata=mydata[mydata['code']==code]
        mydata.fillna('20991231',inplace=True)
        for row in range(len(mydata)):
            entry=mydata.iloc[row]['entry']
            remove=mydata.iloc[row]['remove']
            dataWithIndex.loc[((dataWithIndex['date']>=entry) & (dataWithIndex['date']<=remove)),'industry']=mydata.iloc[row]['industry'][0:4]
            dataWithIndex.loc[((dataWithIndex['date']>=entry) & (dataWithIndex['date']<=remove)),'name']=mydata.iloc[row]['name']
        dataWithIndex.set_index('date',drop=True,inplace=True)
        return dataWithIndex
    #----------------------------------------------------------------------
    @classmethod 
    def __getIndustryFromLocalFile(self):
        exists=os.path.isfile(IndustryClassification.localFileStr)
        if exists==True:
            f=h5py.File(IndustryClassification.localFileStr,'r')
            myKeys=list(f.keys())
            f.close()
            lastStoreDate=datetime.datetime.strptime(max(myKeys), "%Y%m%d")
            if (myKeys==[] or (datetime.datetime.now() - relativedelta(days=+10))>lastStoreDate):#如果10天没有更新，重新抓取数据
                mydata=IndustryClassification.__getAllDataFromOracleServer()
            else:
                store = pd.HDFStore(IndustryClassification.localFileStr,'r')
                mydata=store.select(max(myKeys))
                store.close()
        else:
            mydata=IndustryClassification.__getAllDataFromOracleServer()
        return mydata
    
    #----------------------------------------------------------------------
    @classmethod 
    def __getAllDataFromOracleServer(self):
        oracleConnectString=OracleServer['default']
        oracleStr="select a.S_INFO_WINDCODE as \"code\",a.SW_IND_CODE as industry,e.S_INFO_INDEXCODE as industryCode,e.S_INFO_NAME as \"name\",b.INDUSTRIESNAME as industryName1,c.INDUSTRIESNAME as industryName2,d.INDUSTRIESNAME as industryName3,a.ENTRY_DT as entryDate, a.REMOVE_DT as removeDate from wind_filesync.AShareSWIndustriesClass a,wind_filesync.AShareIndustriesCode b,wind_filesync.AShareIndustriesCode c,wind_filesync.AShareIndustriesCode d,wind_filesync.IndexContrastSector e where substr(a.SW_IND_CODE,1,4)=substr(b.INDUSTRIESCODE,1,4) and b.levelnum = '2' and substr(a.SW_IND_CODE,1,6)=substr(c.INDUSTRIESCODE,1,6)  and c.levelnum = '3'and substr(a.SW_IND_CODE,1,8)=substr(d.INDUSTRIESCODE,1,8)  and d.levelnum = '4'  and substr(e.S_INFO_INDUSTRYCODE,1,4)=substr(a.SW_IND_CODE,1,4) and  substr(e.S_INFO_INDUSTRYCODE,5,10)='000000'  order by a.ENTRY_DT"
        connection = oracle.connect(oracleConnectString)
        cursor = connection.cursor()
        cursor.execute(oracleStr)
        mydata = cursor.fetchall()
        mydata = pd.DataFrame(mydata,columns=['code','industry','industryCode','name','industryName1','industryName2','industryName3','entry','remove'])
        store = pd.HDFStore(IndustryClassification.localFileStr,'a')
        mydata['remove'].fillna(20991231,inplace=True)
        mydata[['entry','remove']] = mydata[['entry','remove']].astype('str')
        store.append(IndustryClassification.nowStr,mydata,append=False,format="table",data_columns=['code','industry','industryCode','name','industryName1','industryName2','industryName3','entry','remove'])
        store.close()
        return mydata  
    #----------------------------------------------------------------------
########################################################################
