from Config.myConfig import *
from Config.myConstant import *
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

########################################################################
class ReturnAnalysis(object):
    """description of class"""
#----------------------------------------------------------------------
    def __init__(self):
        pass
    #----------------------------------------------------------------------
    @classmethod 
    def getBasicInfo(self,myseries):
        numbers=len(myseries)
        winRate=round((myseries>0).astype(int).sum()/numbers,4)*100
        mydata={'mean':myseries.mean(),'median':myseries.median(),'min':myseries.min(),'max':myseries.max(),'std':myseries.std(),'numbers':numbers,'winRate':winRate}
        return mydata
        pass
    #----------------------------------------------------------------------
    @classmethod 
    def getBasicDescribe(self,myseries):
        return myseries.describe()
        pass
    #----------------------------------------------------------------------
    @classmethod 
    def getNetValue(self,days,netvalue,saveAddress,nameStr=EMPTY_STRING):
        nameStr=nameStr+'return'
        fig=plt.figure()
        ax=fig.add_subplot(111)
        plt.plot(days,netvalue)
        plt.xticks(rotation=45)
        plt.xlabel('date')
        plt.ylabel('netvalue')
        plt.grid(True)
        ax.xaxis.set_major_locator(ticker.MultipleLocator(60))
        plt.title(nameStr)
        plt.show()
        plt.legend()
        filePath=saveAddress+'\\'+nameStr
        
        plt.savefig(filePath)
        pass
    #----------------------------------------------------------------------
    @classmethod 
    def getHist(self,myseries,saveAddress,nameStr=EMPTY_STRING):
        nameStr=namestr+'Histogram'
        plt.hist(myseries, bins= 100, range= None, normed= False, weights= None, cumulative= False, bottom= None, histtype= 'bar', align= 'mid', orientation= 'vertical', rwidth= 0.6, log= False, color= None, label= None, stacked= False)
        plt.legend(loc=0)
        plt.grid(True)
        plt.xlabel('Return')
        plt.ylabel('Frequency')
        plt.title(nameStr)
        filePath=saveAddress+'\\'+nameStr
        plt.savefig(filePath)
        pass
########################################################################
