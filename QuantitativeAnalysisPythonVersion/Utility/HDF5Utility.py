import numpy as np
import pandas as pd
import os,sys

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

########################################################################