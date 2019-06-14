import os
from Config.myConstant import *
from Config.myConfig import *
from Utility.HDF5Utility import HDF5Utility
import pandas as pd

########################################################################
class factorBase(object):
    """tick因子的基类"""
    #----------------------------------------------------------------------
    def __init__(self):
        pass
    #----------------------------------------------------------------------
    def clearLocalFile(self,code,date,factor):
        path=os.path.join(LocalFileAddress,'tickFactors',str(factor),str(code))
        fileName=os.path.join(path,str(date)+'.h5')
        HDF5Utility.pathCreate(path)
        exists=os.path.exists(fileName)
        if exists==True:
            os.remove(fileName)
        pass
    def checkLocalFile(self,code,date,factor):
        path=os.path.join(LocalFileAddress,'tickFactors',str(factor),str(code))
        fileName=os.path.join(path,str(date)+'.h5')
        #HDF5Utility.pathCreate(path)
        exists=os.path.exists(fileName)
        return exists
    #----------------------------------------------------------------------
    def updateFactor(self,code,date,factor,data):
        result=data
        if result.shape[0]==0:
            logger.warning(f'There no data of {code} in {date} of factor:{factor}!')
            pass
        else:
            self.saveToLocalFile(code,date,factor,result)
        pass
    #----------------------------------------------------------------------
    def saveToLocalFile(self,code,date,factor,data):
        path=os.path.join(LocalFileAddress,'tickFactors',str(factor),str(code))
        fileName=os.path.join(path,str(date)+'.h5')
        HDF5Utility.pathCreate(path)
        exists=os.path.exists(fileName)
        if exists==False:
            with pd.HDFStore(fileName,'a',complib='blosc:zstd',append=True,complevel=9) as store:
                store.append('data',data,append=False,format="table",data_columns=data.columns)
        pass
    #----------------------------------------------------------------------
    def getDataFromLocalFile(self,code,date,factor):
        path=os.path.join(LocalFileAddress,'tickFactors',str(factor),str(code))
        fileName=os.path.join(path,str(date)+'.h5')
        data=pd.DataFrame()
        exists=os.path.exists(fileName)
        if exists==False:
            logger.warning(f'There is no data of {code}({factor}) in {date} from local file!')
            return data
        with pd.HDFStore(fileName,'r',complib='blosc:zstd',append=True,complevel=9) as store:
            data=store['data']
        return data
        pass
    #----------------------------------------------------------------------
    def getTimeSeriesQuantile(self,mycolumns,data,periods=50,min_periods=20):
        for col in mycolumns:
            data['ts_'+col]=data[col].rolling(periods,min_periods=min_periods).apply((lambda x:pd.Series(x).rank().iloc[-1]/len(x)),raw=True)
        pass
    #----------------------------------------------------------------------
    #check函数，检查因子里面是否存在nan
    def checkDataNan(self,code,date,factor,data):
        errorData=data[data.isna().sum(axis=1)==0]
        if errorData.shape[0]>0:
            logger.error(f'{factor} of {code} in {date} has nan data!!!')
            return False
        else:
            return True
       
########################################################################
