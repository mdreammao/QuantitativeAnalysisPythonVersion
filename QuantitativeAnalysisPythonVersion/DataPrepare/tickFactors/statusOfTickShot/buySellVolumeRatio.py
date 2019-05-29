from Config.myConstant import *
from Config.myConfig import *
from DataPrepare.tickFactors.factorBase import factorBase
from DataAccess.TickDataProcess import TickDataProcess
import pandas as pd
########################################################################
class buySellVolumeRatio(factorBase):
    """描述盘口状态的因子"""
    #----------------------------------------------------------------------
    def __init__(self):
        #super(buySellVolumeRatio,self).__init__()
        super().__init__()
        self.factor='buySellVolumeRatio'
        pass
    def getFactorFromLocalFile(self,code,date):
        mydata=super().getFromLocalFile(code,date,'buySellVolumeRatio')
        return mydata
        pass
    def updateFactor(self,code,date):
        result=self.__computerFactor(code,date)
        if result.shape[0]==0:
            logger.warning(f'There no data of {code} in {date} of factor:{self.factor}!')
            pass
        else:
            super().saveToLocalFile(code,date,self.factor,result)
        pass

    def __computerFactor(self,code,date,data=pd.DataFrame()):
        if data.shape[0]==0:
             data=TickDataProcess().getDataByDateFromLocalFile(code,date)
        result=pd.DataFrame()
        if data!=None:
            mydata=data
            result[['code','date','time']]=mydata[['code','date','time']]
            result['buyVolume']=(mydata['BV1']+mydata['BV2']+mydata['BV3']+mydata['BV4']+mydata['BV5']+mydata['BV6']+mydata['BV7']+mydata['BV8']+mydata['BV9']+mydata['BV10'])
            result['sellVolume']=(mydata['SV1']+mydata['SV2']+mydata['SV3']+mydata['SV4']+mydata['SV5']+mydata['SV6']+mydata['SV7']+mydata['SV8']+mydata['SV9']+mydata['SV10'])
            result['buySellVolumeRatio']=((result['buyVolume']/(result['sellVolume']+result['sellVolume']))-0.5)*2
            result.loc[result['buySellVolumeRatio']==np.inf]['buySellVolumeRatio']=np.nan
            result.fillna(method=ffill)
            result['buySellVolumeRationEMA']=result['buySellVolumeRatio'].ewm(alpha=0.9,ignore_na=False,adjust=True)
            pass
        else:
            logger.error(f'There no data of {code} in {date} to computer factor!') 
        return result()
########################################################################
