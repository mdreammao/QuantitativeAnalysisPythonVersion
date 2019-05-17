import numpy as np
import pandas as pd
import os,sys
from Config.myConfig import *
import h5py


########################################################################
class HDF5Utility(object):
    """description of class"""
    #----------------------------------------------------------------------
    def __init__(self):
        pass
    #----------------------------------------------------------------------
    @classmethod 
    def dataTransfer(self,sourceStr,targetStr):
        source=pd.HDFStore(sourceStr,'a')
        target=pd.HDFStore(targetStr,'a')
        #print(source)
        #print(target)
        sourcekeys=source.keys()
        for code in sourcekeys:
            mycode=code.lstrip("/")
            #print(mycode)
            mydata=source.get(mycode)
            target.append(mycode,mydata,append=False,Format='table')
        source.close()
        target.close()
        os.remove(sourceStr)
        pass
    #----------------------------------------------------------------------
    @classmethod 
    def dataTransferToOneFile(self,sourceStr,targetStr):
        source=pd.HDFStore(sourceStr,'a')
        target=pd.HDFStore(targetStr,'a')
        #print(source)
        #print(target)
        sourcekeys=source.keys()
        for code in sourcekeys:
            mycode=code.lstrip("/")
            #print(mycode)
            mydata=source.get(mycode)
            target.append('all',mydata,append=True,Format='table')
        source.close()
        target.close()
        os.remove(sourceStr)
        pass
    #----------------------------------------------------------------------
    @classmethod 
    def fileClear(self,sourceStr):
        if os.path.exists(sourceStr):
            os.remove(sourceStr)
        pass
    #----------------------------------------------------------------------
    @classmethod 
    def pathCreate(self,path):
        if os.path.exists(path)==False:
            #logger.info(f'{path} is not exists! {path} will be created!')
            try:
                os.makedirs(path)
            except:
                pass
            pass
    #----------------------------------------------------------------------
    @classmethod 
    def fileCheck(self,filePath):
        path=os.path.dirname(filePath)
        HDF5Utility.pathCreate(path)
        exists=os.path.isfile(filePath)
        if exists==True:
            f=h5py.File(filePath,'r')
            myKeys=list(f.keys())
            f.close()
            if myKeys==[]:
                #logger.warning(f'{filePath} has no data!{filePath} will be deleted!')
                os.remove(filePath)
                exists=False
            pass
        return exists
        pass
########################################################################