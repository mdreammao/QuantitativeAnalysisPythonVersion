import os
from Config.myConstant import *
from Config.myConfig import *
from Utility.HDF5Utility import HDF5Utility
from DataAccess.StockIPOInfoProcess import StockIPOInfoProcess
from DataAccess.TradedayDataProcess import TradedayDataProcess
import pandas as pd
import datetime 

########################################################################
class factorBase(object):
    """日频因子的基本处理"""
    #----------------------------------------------------------------------
    def __init__(self):
        self.listDate=EMPTY_STRING
        self.delistDate=EMPTY_STRING
        pass
    #----------------------------------------------------------------------
    def setIPODate(self,code):
        if self.listDate==EMPTY_STRING:
            IPOInfo=StockIPOInfoProcess.getStockIPOInfoByCode(code)
            self.listDate=IPOInfo['listDate'].iloc[0]
            self.delistDate=IPOInfo['delistDate'].iloc[0]
        return [self.listDate,self.delistDate]

    #----------------------------------------------------------------------
    def getPath(self,code,factor):
        path=os.path.join(LocalFileAddress,'dailyFactors',str(factor))
        return path
        pass
    #----------------------------------------------------------------------
    def getFileName(self,code,factor):
        path=os.path.join(LocalFileAddress,'dailyFactors',str(factor))
        fileName=os.path.join(path,str(code).replace('.','_')+'.h5')
        return fileName
        pass
    #----------------------------------------------------------------------
    def clearLocalFile(self,code,factor):
        HDF5Utility.pathCreate(self.getPath(code,factor))
        fileName=self.getFileName(code,factor)
        exists=os.path.exists(fileName)
        if exists==True:
            os.remove(fileName)
        pass
    #----------------------------------------------------------------------
    def getNeedToUpdateDaysOfFactor(self,code,factor):
        self.setIPODate(code)
        listDate=self.listDate
        delistDate=self.delistDate
        today=datetime.datetime.now().strftime("%Y%m%d")
        yesterday=TradedayDataProcess.getPreviousTradeday(today)
        endDate=min(delistDate,yesterday)
        lastDate=self.getLastDate(code,factor)
        if lastDate!=EMPTY_STRING:
            startDate=max(listDate,TradedayDataProcess.getNextTradeday(lastDate))
        else:
            startDate=listDate
        return [startDate,endDate]
        pass

    #----------------------------------------------------------------------
    def getLastDate(self,code,factor):
        exists=self.checkLocalFile(code,factor)
        fileName=self.getFileName(code,factor)
        lastDate=EMPTY_STRING
        if exists==True:
            with pd.HDFStore(path=fileName,mode='r',complib='blosc:zstd',append=True,complevel=9) as store:
                days=store['date']
                lastDate=days.max()
            pass
        return lastDate
        pass
    #----------------------------------------------------------------------
    def checkLocalFile(self,code,factor):
        path=self.__getPath(code,factor)
        HDF5Utility.pathCreate(path)
        fileName=self.__getFileName(code,factor)
        exists=os.path.exists(fileName)
        return exists
    #----------------------------------------------------------------------
    def updateFactor(self,code,factor,data):
        if data.shape[0]==0:
            logger.warning(f'There no data of {code} of factor:{factor}!')
            pass
        else:
            lastDate=self.getLastDate(code,factor)
            data=data[data['date']>lastDate]
            if data.shape[0]>0:
                self.saveToLocalFile(code,factor,data)
            else:
                logger.warning(f'There no data of {code} of factor:{factor}!')
                pass
        pass

    #----------------------------------------------------------------------
    def saveToLocalFile(self,code,factor,data):
        path=self.__getPath(code,factor)
        fileName=self.__getFileName(code,factor)
        HDF5Utility.pathCreate(path)
        exists=os.path.exists(fileName)
        #如果存在原数据,和原始数据一起合并存储
        if exists==True:
            with pd.HDFStore(fileName,'r',complib='blosc:zstd',append=True,complevel=9) as store:
                oldData=store['data']
                data=pd.concat([oldData,data])
        with pd.HDFStore(fileName,'a',complib='blosc:zstd',append=False,complevel=9) as store:
            store.append('data',data,append=False,format="table",data_columns=data.columns)
            store.append('date',data['date'],append=False,format="table",data_columns=data.columns)
        pass
    #----------------------------------------------------------------------
    def getDataFromLocalFile(self,code,factor):
        path=self.__getPath(code,factor)
        fileName=self.__getFileName(code,factor)
        data=pd.DataFrame()
        exists=os.path.exists(fileName)
        if exists==False:
            logger.warning(f'There is no data of {code}({factor}) from local file!')
            return data
        with pd.HDFStore(fileName,'r',complib='blosc:zstd',append=True,complevel=9) as store:
            data=store['data']
        return data
        pass

########################################################################