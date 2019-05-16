import pymssql
import cx_Oracle as oracle
import pandas as pd
from Config.myConstant import *
from Config.myConfig import *
import datetime
import h5py
import os
from DataAccess.TradedayDataProcess import *
from Utility.JobLibUtility import *
from Utility.HDF5Utility import *
import pymssql

########################################################################
class TickDataProcess(object):
    """从170数据库获取TICK数据"""
    #----------------------------------------------------------------------
    def __init__(self,record=False,SqlSource=SqlServer['server170']):
        """Constructor"""
        self.record=record
        strArry=SqlSource.split(';')
        self.address=strArry[0].split('=')[1]
        self.user=strArry[1].split('=')[1]
        self.password=strArry[2].split('=')[1]
    #----------------------------------------------------------------------
    def getResampleTickShotData(self,code,date):
        mydata=self.getTickShotDataFromSqlServer(code,date)
        mydata=mydata.resample('3s',label='right',closed='right').last()
        mydata=mydata.fillna(method='ffill')
        mydata['volumeIncrease']=mydata['volume']-mydata['volume'].shift(1)
        mydata['amountIncrease']=mydata['amount']-mydata['amount'].shift(1)
        mydata=mydata[((mydata.index.time>=datetime.time(9,30)) & (mydata.index.time<=datetime.time(11,30))) | ((mydata.index.time>=datetime.time(13,00)) & (mydata.index.time<=datetime.time(15,00)))]
        return mydata
        pass
    #----------------------------------------------------------------------
    def getTickShotDataFromSqlServer(self,code,date):
        date=str(date)
        month=date[0:6]
        database='WindFullMarket'+month
        table='MarketData_'+code.replace('.','_')
        connect=pymssql.connect( self.address,self.user,self.password,database,charset='utf8')
        cursor = connect.cursor()
        sql="select [stkcd],[tdate],[ttime],[cp],[S1],[S2],[S3],[S4],[S5],[B1],[B2],[B3],[B4],[B5],[SV1],[SV2],[SV3],[SV4],[SV5],[BV1],[BV2],[BV3],[BV4],[BV5],[ts],[tt] FROM [{0}].[dbo].[{1}] where [tdate]={2} and ((ttime>=92500000 and ttime<=113000000) or (ttime>=130000000 and ttime<=150000000)) order by ttime".format(database,table,date)
        cursor.execute(sql)
        mydata=cursor.fetchall()
        mydata = pd.DataFrame(mydata,columns=['code' ,'date','time' ,'lastPrice','S1','S2','S3','S4','S5','B1','B2','B3','B4','B5','SV1','SV2','SV3','SV4','SV5','BV1','BV2','BV3','BV4','BV5','volume' ,'amount'])
        mydata[['lastPrice','S1','S2','S3','S4','S5','B1','B2','B3','B4','B5','SV1','SV2','SV3','SV4','SV5','BV1','BV2','BV3','BV4','BV5','volume' ,'amount']] = mydata[['lastPrice','S1','S2','S3','S4','S5','B1','B2','B3','B4','B5','SV1','SV2','SV3','SV4','SV5','BV1','BV2','BV3','BV4','BV5','volume' ,'amount']].astype('float')
        mydata['mytime']=pd.to_datetime(mydata['date']+mydata['time'],format='%Y%m%d%H%M%S%f')
        mydata.set_index('mytime',inplace=True,drop=True)
        return mydata    

        pass
    