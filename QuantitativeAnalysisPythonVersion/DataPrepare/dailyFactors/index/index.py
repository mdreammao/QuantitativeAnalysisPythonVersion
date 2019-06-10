from DataPrepare.dailyFactors.factorBase import factorBase 
from DataAccess.IndexComponentDataProcess import IndexComponentDataProcess
from DataAccess.TradedayDataProcess import TradedayDataProcess
from Config.myConstant import *
from Config.myConfig import *
########################################################################
class index(factorBase):
    """处理和指数相关的日线因子"""
    #----------------------------------------------------------------------
    def __init__(self):
        super().__init__()
        self.factor='index'
        pass
    #----------------------------------------------------------------------
    def updateFactor(self,code):
        exists=super().checkLocalFile(code,self.factor)
        [startDate,endDate]=super().getNeedToUpdateDaysOfFactor(code,self.factor)
        if endDate<startDate:#无需更新
            return 
        result=self.__computerFactor(code,startDate,endDate)
        super().updateFactor(code,self.factor,result)
    #----------------------------------------------------------------------
    #给定原始数据和日期进行计算
    def __computerFactor(self,code,startDate,endDate):  
        myindex=IndexComponentDataProcess()
        myIndexBelongs50=myindex.getStockBelongs(code,SSE50,startDate,endDate)
        myIndexBelongs300=myindex.getStockBelongs(code,HS300,startDate,endDate)
        myIndexBelongs500=myindex.getStockBelongs(code,CSI500,startDate,endDate)
        myStockWeightOf50=IndexComponentDataProcess.getStockPropertyInIndex(code,SSE50,startDate,endDate)
        myStockWeightOf300=IndexComponentDataProcess.getStockPropertyInIndex(code,HS300,startDate,endDate)
        myStockWeightOf500=IndexComponentDataProcess.getStockPropertyInIndex(code,CSI500,startDate,endDate)
        tradedays=TradedayDataProcess.getTradedays(startDate,endDate)
        mydata=pd.DataFrame(data=tradedays)
        mydata.set_index('date',drop=True,inplace=True)
        mydata['is50']=myIndexBelongs50['exists']
        mydata['is300']=myIndexBelongs300['exists']
        mydata['is500']=myIndexBelongs500['exists']
        mydata['weight50']=myStockWeightOf50['weight']
        mydata['weight300']=myStockWeightOf300['weight']
        mydata['weight500']=myStockWeightOf500['weight']
        mydata.reset_index(drop=False,inplace=True)
        mycolumns=['date','is50','is300','is500','weight50','weight300','weight500']
        mydata=mydata[mycolumns]
        return mydata
        pass
########################################################################