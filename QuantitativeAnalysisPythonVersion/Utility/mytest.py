import pandas as pd
import numpy as np
import datetime
import numba

########################################################################
class mytest(object):
    """description of class"""
    def __init__(self):
        pass
    def testnumba(self):
        m=pd.DataFrame(np.arange(100000000).reshape(10000000,10))
        m=m.as_matrix()
        #@numba.jit(numba.float64[:](numba.float64[:,:],numba.int64),nopython=True)
        @numba.jit(nopython=True,parallel=True)
        def f(m,window):
            maxlen=len(m)
            z=[]
            for i in range(0,len(m)):
                tmp1=0
                tmp2=0
                for j in range(0,min(window,maxlen-i)):
                    tmp1=tmp1+m[i+j][0]
                    tmp2=tmp2+m[i+j][1]
                z.append(tmp1/tmp2)
            return z
        print(datetime.datetime.now())
        z=f(m,240)
        print(len(z))
        '''
        for i in range(0,len(m)):
            tmp1=0
            tmp2=0
            for j in range(0,min(240,maxlen-i)):
                tmp1=tmp1+m[i+j][0]
                tmp2=tmp2+m[i+j][1]
            z.append(tmp1/tmp2)
        '''
        print(datetime.datetime.now())
        pass


########################################################################