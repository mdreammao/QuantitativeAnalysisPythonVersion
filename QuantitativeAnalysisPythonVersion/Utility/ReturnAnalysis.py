from Config.myConfig import *
from Config.myConstant import *
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import seaborn as sns

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
        mydata={'mean':myseries.mean(),'median':myseries.median(),'min':myseries.min(),'max':myseries.max(),'std':myseries.std(),'numbers':numbers,'winRate':winRate,'count':numbers}
        return mydata
        pass
    #----------------------------------------------------------------------
    @classmethod 
    def getBasicDescribe(self,myseries):
        return myseries.describe()
        pass
    
    #----------------------------------------------------------------------
    @classmethod 
    def getBar(self,x,y,xname,yname,saveAddress,nameStr=EMPTY_STRING):
        # 创建一个点数为 8 x 6 的窗口, 并设置分辨率为 80像素/每英寸
        plt.figure()
        plt.rcParams['font.sans-serif']=['SimHei']
        plt.rcParams['axes.unicode_minus'] = False
        # 再创建一个规格为 1 x 1 的子图
        plt.subplot(1, 1, 1)
        # 柱子总数
        N = 6
        # 包含每个柱子对应值的序列
        values = y
        # 包含每个柱子下标的序列
        index = list(range(0,len(x)))
        # 柱子的宽度
        width = 0.35
        # 绘制柱状图, 每根柱子的颜色为紫罗兰色
        p2 = plt.bar(index, values, width, label=nameStr, color="#87CEFA")
        # 设置横轴标签
        plt.xlabel(xname)
        # 设置纵轴标签
        plt.ylabel(yname)
        # 添加标题
        plt.title(nameStr)
        # 添加纵横轴的刻度
        plt.xticks(index,list(x))
        plt.xticks(rotation=45)
        #plt.yticks(np.arange(0, 81, 10))
        # 添加图例
        #plt.legend(loc="upper right")
        #plt.show()
        filePath=saveAddress+'\\'+nameStr
        plt.tight_layout()
        plt.savefig(filePath)
        
    #----------------------------------------------------------------------
    @classmethod 
    def getBar2(self,mydata):
        sns.set(style="darkgrid")
        (fx,fy)=(1200,1200)
        my_dpi=300
        fig=plt.figure(figsize=(fx/my_dpi, fy/my_dpi), dpi=my_dpi)
        plt.rcParams['font.sans-serif']=['SimHei']
        plt.rcParams['axes.unicode_minus'] = False
        ax = fig.add_subplot(1,1,1)

        ax = sns.barplot(x="name", 
                         y='mean', 
                         data=mydata)

        ax.set_ylabel("mean")
        plt.xticks(rotation=45)
        #plt.savefig('../figures/barplot0221', bbox_inches='tight',pad_inches = 0)
        plt.show()
        plt.close(fig)
        plt.clf()



    #----------------------------------------------------------------------
    @classmethod 
    def getNetValue(self,days,netvalue,saveAddress,nameStr=EMPTY_STRING):
        nameStr=nameStr+'return'
        fig=plt.figure()
        ax=fig.add_subplot(111)
        plt.plot(days,netvalue)
        plt.xticks(rotation=30)
        plt.xlabel('date')
        plt.ylabel('netvalue')
        plt.grid(True)
        ax.xaxis.set_major_locator(ticker.MultipleLocator(200))
        plt.title(nameStr)
        #plt.show()
        #plt.legend()
        filePath=saveAddress+'\\'+nameStr
        plt.tight_layout()
        plt.savefig(filePath)
        
        pass
    #----------------------------------------------------------------------
    @classmethod 
    def getHist(self,myseries,saveAddress,nameStr=EMPTY_STRING):
        nameStr=nameStr+'Histogram'
        fig=plt.figure()
        plt.hist(myseries, bins= 100, range= None, normed= False, weights= None, cumulative= False, bottom= None, histtype= 'bar', align= 'mid', orientation= 'vertical', rwidth= 0.6, log= False, color= None, label= None, stacked= False)
        plt.legend(loc=0)
        plt.grid(True)
        plt.xlabel('Return')
        plt.ylabel('Frequency')
        plt.title(nameStr)
        filePath=saveAddress+'\\'+nameStr
        plt.tight_layout()
        plt.savefig(filePath)
        pass
########################################################################
