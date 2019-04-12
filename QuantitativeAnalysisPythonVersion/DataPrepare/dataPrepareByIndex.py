from Config.myConstant import *
from Config.myConfig import *
from DataAccess.IndexComponentDataProcess import *
from DataAccess.KLineDataProcess import *
from DataAccess.TradedayDataProcess import *
from DataAccess.StockSharesProcess import *
import time



########################################################################
class dataPrepareByIndex(object):
    """description of class"""
#----------------------------------------------------------------------
    def __init__(self):
        pass
#----------------------------------------------------------------------
    def getStockData(self):
        myindex=IndexComponentDataProcess(True)
        index500=myindex.getCSI500DataByDate(20190404,20190404)
        index300=myindex.getHS300DataByDate(20190404,20190404)
        index50=myindex.getSSE50DataByDate(20190404,20190404)
        stockCodes=list(pd.concat([index500,index300,index50],ignore_index=True)['code'].drop_duplicates())
        myMinute=KLineDataProcess('minute',True)
        myDaily=KLineDataProcess('daily',True)
        num=0
        

        for code in stockCodes:
            print(datetime.datetime.now())
            num=num+1
            myMinute.getDataByDate(code,20070101,20190327)
            myDaily.getDataByDate(code,20070101,20190327)
            print("{0}({1} of 800) complete!".format(code,num))
########################################################################