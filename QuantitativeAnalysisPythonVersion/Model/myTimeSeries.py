from Config.myConstant import *

########################################################################
class myTimeSeries(object):
    """时间序列"""
     #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        self.fulltime = EMPTY_STRING         #时间  yyyy-MM-dd-hh-mm-ss.00000格式  
        self.date=EMPTY_INT
        self.time=EMPTY_INT
        self.rawData = None                     # 原始数据           
        
        
########################################################################
