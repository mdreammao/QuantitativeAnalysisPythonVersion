from DataAccess.IndexComponentDataProcess import *
from DataAccess.KLineDataProcess import *
from DataAccess.TradedayDataProcess import *
from DataPrepare.dailyFactorsProcess import *
from DataPrepare.tickFactorsProcess import *
from DataAccess.TickDataProcess import *
from Utility.JobLibUtility import *
from Utility.TradeUtility import *
import warnings
from Config.myConstant import *
from Config.myConfig import *
import numpy as np
import os
import copy
########################################################################
class stockReverseByStdOnTick(object):
    """股票异动,专注股票大涨之后的回调"""
    #----------------------------------------------------------------------
    def __init__(self):
        pass
    #----------------------------------------------------------------------
    def reverse_multipleCodes_parallel(self,codes,startDate,endDate,parameters=[300,100000000,1.5]):
        mydata=JobLibUtility.useJobLibToComputeByCodes(self.reverse_multipleCodes,codes,MYGROUPS,startDate,endDate,parameters)
        return mydata
        pass
    #----------------------------------------------------------------------
    #输入code=600000.SH，startdate=yyyyMMdd，endDate=yyyyMMdd
    def reverse_multipleCodes(self,codes,startDate,endDate,parameters=[300,100000000,1.5]):
        mydata=[]
        for i in range(len(codes)):
            code=codes[i]
            data=self.reverse_singleCode(code,startDate,endDate,parameters=[300,100000000,1.5])
            mydata.append(data)
        mydata=pd.concat(mydata)
        return mydata
    #----------------------------------------------------------------------
    def reverse_singleCode(self,code,startDate,endDate,parameters=[300,100000000,1.5]):
        myindex=parameters[0]
        totalCash=parameters[1]
        std1=parameters[2]
        days=list(TradedayDataProcess().getTradedays(startDate,endDate))
        #factors=['closeStd','index','marketValue','industry']
        dailyRepo=dailyFactorsProcess()
        dailyFactor=dailyRepo.getSingleStockDailyFactors(code,startDate,endDate)
        dailyKLine=KLineDataProcess('daily')
        dailyData=dailyKLine.getDataByDate(code,startDate,endDate)
        if dailyData.empty==True:
            logger.error(f'there is no data of {code} from {startDate} to {endDate}')
            return pd.DataFrame()
        tick=TickDataProcess()
        mydata=[]
        position=0
        profit=0
        myStatusList=[]
        myTradeList=[]
        myStatus={}
        myTrade={}
        positionYesterday=0
        for today in days:
            #logger.info(f'{code} in {today} start!')
            todayInfo=dailyFactor[dailyFactor['date']==today]
            if todayInfo.empty==True:
                logger.error(f'there is no factor data of {code} in date {today}')
                continue
                pass

            todayKLine=dailyData[dailyData['date']==today]
            if todayKLine.empty==True:
                logger.error(f'there is no KLine data of {code} in date {today}')
                continue
                pass
            myStatus['date']=today
            myStatus['closeStd20']=todayInfo['closeStd20'].iloc[0]
            myStatus['weight50']=todayInfo['weight50'].iloc[0]
            myStatus['weight300']=todayInfo['weight300'].iloc[0]
            myStatus['weight500']=todayInfo['weight500'].iloc[0]
            myStatus['ts_closeStd20']=todayInfo['closeStd20'].iloc[0]
            myStatus['adjFactor']=todayKLine['adjFactor'].iloc[0]
            myStatus['preClose']=todayKLine['preClose'].iloc[0]
            positionNow=positionYesterday
            if (todayInfo.empty==False) & (todayKLine['status'].iloc[0]!='停牌'):
                if myindex==300:
                    maxPosition=myStatus['weight300']*totalCash*0.01/myStatus['preClose']
                elif myindex==500:
                    maxPosition=myStatus['weight300']*totalCash*0.01/myStatus['preClose']
                elif myindex==50:
                    maxPosition=myStatus['weight50']*totalCash*0.01/myStatus['preClose']
                else:
                    maxPosition=myStatus['totalCash']*0.001/myStatus['preClose']
                maxPosition=round(maxPosition,-2)
                if maxPosition==0:
                    continue
                tickData=tick.getDataByDateFromLocalFile(code,today)
                #['code' ,'date','tick' ,'lastPrice','S1','S2','S3','S4','S5','S6','S7','S8','S9','S10','B1','B2','B3','B4','B5','B6','B7','B8','B9','B10','SV1','SV2','SV3','SV4','SV5','SV6','SV7','SV8','SV9','SV10','BV1','BV2','BV3','BV4','BV5','BV6','BV7','BV8','BV9','BV10','volume' ,'amount','volumeIncrease','amountIncrease']
                tickList=tickData.as_matrix()
                for i in range(0,len(tickList)-60):
                    now=tickList[i]
                    midPrice=(now[4]+now[14])/2
                    lastPrice=now[3]
                    tickShot=now[4:43]
                    upCeiling=False
                    downCeiling=False
                    if now[24]==0:
                        upCeiling=True
                        pass
                    if now[34]==0:
                        downCeiling=True
                        pass
                    mytime=datetime.datetime.strptime(now[1]+now[2],'%Y%m%d%H%M%S%f')  
                    increaseToday=tickList[i][3]/myStatus['preClose']-1
                    if (i>=100) & (tickList[i-100][3]>0):
                        increase5m=tickList[i][3]/tickList[i-100][3]-1
                    else:
                        increase5m=np.nan
                    if ((positionNow==0) &(positionYesterday==0) &(i<=4500) &(i>=100)& (increaseToday>std1*myStatus['closeStd20']) & (maxPosition>0)& (downCeiling==False)):
                        #开空头
                        [price,deltaPosition,amount]=TradeUtility.sellByTickShotData(tickShot,maxPosition,0.001)
                        positionNow=-deltaPosition
                        myTrade['date']=today
                        myTrade['opentime']=mytime
                        myTrade['position']=positionNow
                        myTrade['open']=price
                        myTrade['openAdj']=myStatus['adjFactor']
                        myTrade['increase5m']=increase5m
                        myTrade['increaseToday']=increaseToday
                        openIndex=i
                        maxPosition=maxPosition-deltaPosition
                        pass
                    elif ((positionNow==0) &(positionYesterday==0) &(i<=4500)&(i>=100) & (increaseToday<-std1*myStatus['closeStd20']) & (maxPosition>0) & (upCeiling==False)):
                        #开多头
                        [price,deltaPosition,amount]=TradeUtility.buyByTickShotData(tickShot,maxPosition,0.001)
                        positionNow=deltaPosition
                        myTrade['date']=today
                        myTrade['opentime']=mytime
                        myTrade['position']=positionNow
                        myTrade['open']=price
                        myTrade['openAdj']=myStatus['adjFactor']
                        myTrade['increase5m']=increase5m
                        myTrade['increaseToday']=increaseToday
                        maxPosition=maxPosition-deltaPosition
                        openIndex=i
                        pass
                    elif (positionNow>0):
                        profit=(lastPrice*myStatus['adjFactor']-myTrade['open']*myTrade['openAdj'])/(myTrade['open']*myTrade['openAdj'])
                        if (((profit<0) & (i>=openIndex+400)) |(i>=4700)|(i>=openIndex+1200)|(positionYesterday!=0)):
                            if downCeiling==False: #未跌停
                                #平多头,记录一笔交易
                                [price,deltaPosition,amount]=TradeUtility.sellByTickShotData(tickShot,positionNow,0.1) 
                                myTrade['closetime']=mytime
                                myTrade['close']=price
                                myTrade['closeAdj']=myStatus['adjFactor']
                                positionNow=0
                                positionYesterday=0
                                openIndex=0
                                tradeCopy=copy.deepcopy(myTrade)
                                myTradeList.append(tradeCopy)
                                myTrade={}
                            pass
                    elif (positionNow<0):
                        profit=(-lastPrice*myStatus['adjFactor']+myTrade['open']*myTrade['openAdj'])/(myTrade['open']*myTrade['openAdj'])
                        if (((profit<0) & (i>=openIndex+400)) |(i>=4700) |(i>=openIndex+1200)|(positionYesterday!=0)):
                            if upCeiling==False:
                                #平空头，记录一笔交易
                                [price,deltaPosition,amount]=TradeUtility.buyByTickShotData(tickShot,-positionNow,0.1)
                                myTrade['closetime']=mytime
                                myTrade['close']=price
                                myTrade['closeAdj']=myStatus['adjFactor']
                                positionNow=0
                                positionYesterday=0
                                openIndex=0
                                tradeCopy=copy.deepcopy(myTrade)
                                myTradeList.append(tradeCopy)
                                myTrade={}
                                
                            pass
                if positionNow!=0:
                    logger.info(f'{code} of {today} can not close the position!')
                positionYesterday=positionNow
                pass
            else:
                logger.warning(f'There is no data of {code} in {today}')
        m=pd.DataFrame(data=myTradeList,columns=['date','opentime','position','open','openAdj','increase5m','increaseToday','closetime','close','closeAdj'])
        
        m['profit']=m['position']*(m['close']*m['closeAdj']-m['open']*m['openAdj'])/m['openAdj']
        m['fee']=m['position'].abs()*m[['open','close']].max(axis=1)*0.0012
        m['netProfit']=m['profit']-m['fee']
        m['code']=code
        mycolumns=['date','closeStd20','ts_closeStd20','industry','industryName']
        dailyFactor=dailyFactor[mycolumns]
        mydata=pd.merge(m,dailyFactor,how='left',left_on='date',right_on='date')
        #mydata=mydata[((mydata['increase5m']>mydata['closeStd20']) & (mydata['position']<0)) |((mydata['increase5m']<-mydata['closeStd20']) & (mydata['position']>0))]
        '''
        print(mydata)
        print(mydata['profit'].sum())
        print(mydata['fee'].sum())
        print(mydata['netProfit'])
        '''
        return mydata
########################################################################