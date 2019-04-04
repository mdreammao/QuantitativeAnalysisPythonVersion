from Config.myConstant import *
from Config.myConfig import *
from DataAccess.IndexComponentDataProcess import *
from DataAccess.KLineDataProcess import *
from DataAccess.TradedayDataProcess import *
from DataPrepare.dataPrepareByIndex import *
import time

#----------------------------------------------------------------------
def main():
    """主程序入口"""
    prepare=dataPrepareByIndex()
    prepare.getStockData()



if __name__ == '__main__':
    main()
