from myTimeSeries import *
from Config.myConstant import *

########################################################################
class myMinuteKLine(myTimeSeries):
    """成交数据类"""

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        super(MinuteKLine, self).__init__()
        #代码编号相关
        self.code=  EMPTY_STRING
        #行情相关
        self.high=EMPTY_FLOAT
        self.open=EMPTY_FLOAT
        self.low=EMPTY_FLOAT
        self.close=EMPTY_FLOAT
        self.volume=EMPTY_FLOAT
        self.amount=EMPTY_FLOAT

########################################################################
