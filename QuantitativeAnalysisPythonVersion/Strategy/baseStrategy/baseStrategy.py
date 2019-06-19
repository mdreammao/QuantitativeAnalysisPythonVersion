from Config.myConstant import *
from Config.myConfig import *
from DataAccess.TradedayDataProcess import TradedayDataProcess
from DataPrepare.tickFactorsProcess import tickFactorsProcess
from Utility.JobLibUtility import JobLibUtility
import pandas as pd
from typing import List as type_list

########################################################################
class baseStrategy(object):
    """策略回测的基类"""
    #----------------------------------------------------------------------
    def __init__(self):
        pass
    #----------------------------------------------------------------------
    def multipleCodes_parallel(self,codes:type_list[str],startDate:str,endDate:str,parameters:list):
        raise NotImplementedError()
    #----------------------------------------------------------------------
    def multipleCodes(self,codes:type_list[str],startDate:str,endDate:str,parameters:list):
        raise NotImplementedError()
    #----------------------------------------------------------------------
    def singleCode(self,code:str,startDate:str,endDate:str,parameters:list):
        raise NotImplementedError()
    #----------------------------------------------------------------------
    def strategy():
        raise NotImplementedError()

########################################################################

