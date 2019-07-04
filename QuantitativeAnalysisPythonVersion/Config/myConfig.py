import logging
import os
import time
import datetime
from Utility.HDF5Utility import *


########################################################################
#定义多线程数据
MYJOBS=-1
MYGROUPS=400
####################################################################
#日频因子的参数配置
#使用到的日频因子
DAILYFACTORSUSED=[
    {'module': 'DataPrepare.dailyFactors.index.index', 'class': 'index', 'factor': 'index'},
    {'module': 'DataPrepare.dailyFactors.industry.industry', 'class': 'industry', 'factor': 'industry'},
    {'module': 'DataPrepare.dailyFactors.marketValue.marketValue', 'class': 'marketValue', 'factor': 'marketValue'},
    {'module': 'DataPrepare.dailyFactors.volatility.closeStd', 'class': 'closeStd', 'factor': 'closeStd'}
    ]

#需要计算的日频因子
DAILYFACTORSNEEDTOUPDATE=[
    {'module': 'DataPrepare.dailyFactors.index.index', 'class': 'index', 'factor': 'index'},
    {'module': 'DataPrepare.dailyFactors.industry.industry', 'class': 'industry', 'factor': 'industry'},
    {'module': 'DataPrepare.dailyFactors.marketValue.marketValue', 'class': 'marketValue', 'factor': 'marketValue'},
    {'module': 'DataPrepare.dailyFactors.volatility.closeStd', 'class': 'closeStd', 'factor': 'closeStd'}
    ]

####################################################################
#tick因子的参数配置

#使用到的tick因子
TICKFACTORSUSED=[
    {'module': 'DataPrepare.tickFactors.statusOfTickShot.buySellForce', 'class': 'buySellForce', 'factor': 'buySellForce'},
    {'module': 'DataPrepare.tickFactors.changeOfTickShot.midPriceChange', 'class': 'midPriceChange', 'factor': 'midPriceChange'},
    {'module': 'DataPrepare.tickFactors.targetFactor.targetFactor', 'class': 'targetFactor', 'factor': 'targetFactor'}
    ]

#需要计算的tick因子
TICKFACTORSNEEDTOUPDATE=[
    {'module': 'DataPrepare.tickFactors.statusOfTickShot.buySellForce', 'class': 'buySellForce', 'factor': 'buySellForce'},
    {'module': 'DataPrepare.tickFactors.changeOfTickShot.midPriceChange', 'class': 'midPriceChange', 'factor': 'midPriceChange'},
    {'module': 'DataPrepare.tickFactors.targetFactor.targetFactor', 'class': 'targetFactor', 'factor': 'targetFactor'}
    ]
####################################################################
#sql连接字符串
SqlServer={
    'server170':'server=192.168.1.170;uid=reader;pwd=reader;',
    'local':'server=(local);uid=sa;pwd=maoheng0;'
    }


#oracle连接字符串
OracleServer={
    'default':'yspread/Y*iaciej123456@172.17.21.3:1521/WDZX',
    }

#infludexdb连接字符串
InfluxdbServer={
    'host':'172.17.58.71',
    'port':8086,
    'username':'root', 
    'password':'root',
    'database':''
    }

InfluxdbServerInternet={
    'host':'192.168.58.71',
    'port':8086,
    'username':'root', 
    'password':'root',
    'database':''
    }

#本地文件存储地址
#LocalFileAddress=r'D:/BTP/LocalDataBase'
LocalFileAddress=r'/home/public/mao/BTP/LocalDataBase'
#ROOT_PATH = '/home/orientsec38/QuantitativeAnalysisPythonVersion/QuantitativeAnalysisPythonVersion/BTP'

#LocalFileAddress=os.path.join(ROOT_PATH, 'LocalDataBase')


####################################################################
#logger日志文件
# 创建一个logger
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)  # Log等级总开关
# 创建一个handler，用于写入日志文件
rq = time.strftime('%Y%m%d%H%M', time.localtime(time.time()))
localFilePath=os.path.join(LocalFileAddress,'log')
HDF5Utility.pathCreate(localFilePath)
logfile=os.path.join(LocalFileAddress,'log',datetime.datetime.strftime(datetime.datetime.now(),'%Y%m%d%H%M%S')+'.log')
fh = logging.FileHandler(logfile, mode='w')
fh.setLevel(logging.DEBUG)  # 输出到file的log等级的开关
# 定义handler的输出格式
formatter = logging.Formatter("%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")
fh.setFormatter(formatter)
# 将logger添加到handler里面
logger.addHandler(fh)
#创建一个handler，用于输出到控制台
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(formatter)
logger.addHandler(ch)
####################################################################

