import pandas as pd
########################################################################
class machineLeariningBase(object):
    """机器学习基类"""
    #----------------------------------------------------------------------
    def __init__(self):
        pass
    #----------------------------------------------------------------------
    def getData(self,fileName):
        data=pd.DataFrame()
        with pd.HDFStore(fileName,'r',complib='blosc:zstd',append=True,complevel=9) as store:
            data=store['data']
        return data
        pass
########################################################################