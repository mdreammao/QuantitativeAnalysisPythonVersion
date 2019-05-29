import os
from Config.myConstant import *
from Config.myConfig import *
from HDF5Utility import HDF5Utility
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
    def getFromLocalFile(self,code,date,factor):
        path=os.path.join(LocalFileAddress,'tickFactors',str(factor),str(code))
        fileName=os.path.join(path,str(date)+'.h5')
        data=pd.DataFrame()
        exists=os.path.exists(fileName)
        if exists==False:
            return data
        with pd.HDFStore(fileName,'r',complib='blosc:zstd',append=True,complevel=9) as store:
            data=store['data']
        return data
        pass
    
########################################################################
