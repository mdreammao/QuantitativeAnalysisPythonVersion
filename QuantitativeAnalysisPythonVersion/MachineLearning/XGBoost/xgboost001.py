from MachineLearning.machineLeariningBase import machineLeariningBase
import xgboost as xgb
from xgboost import plot_importance
from matplotlib import pyplot as plt
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
import os
from Config.myConfig import *
########################################################################
class xgboost001(machineLeariningBase):
    """xgboost测试"""
    #----------------------------------------------------------------------
    def __init__(self,document):
        self.path=os.path.join(LocalFileAddress,document)
        return super().__init__()
    #----------------------------------------------------------------------
    def mytrain(self,startDate,endDate,testStartDate,testEndDate):
        data=super().getDataFromLocal(self.path,startDate,endDate)
        testData=super().getDataFromLocal(self.path,testStartDate,testEndDate)
        # 加载数据集,此数据集时做回归的
        featuresCol = ['buyForce','sellForce','buySellForceChange','buySellSpread',
                'differenceHighLow3m','midToVwap','midToVwap3m','midPrice3mIncrease','midPriceBV3m', 'midInPrevious3m', 
                'midStd60', 'increaseToday','closeStd20',
                'buySellVolumeRatio2', 'buySellWeightedVolumeRatio2',
                'buySellVolumeRatio5','buySellWeightedVolumeRatio5',
                 'buySellVolumeRatio10','buySellWeightedVolumeRatio10']
        targetCol=['midIncreaseNext1m','midIncreaseMinNext1m','midIncreaseMaxNext1m','midIncreaseMinNext2m','midIncreaseMaxNext2m','midIncreaseMinNext5m','midIncreaseMaxNext5m']
        # 算法参数
        params = {
            'booster':'gbtree',
            'objective':'reg:linear',
            'gamma':0.1,
            'max_depth':5,
            'lambda':3,
            'subsample':0.7,
            'colsample_bytree':0.7,
            'min_child_weight':3,
            'slient':0,
            'eta':0.1,
            'seed':1000,
        }
        for target in targetCol:
            logger.info(f'Training {target} by xgboost start!!!!')
            y= data[target].values
            X = data[featuresCol].values
            ytest=testData[target].values
            Xtest=testData[featuresCol].values
            # Xgboost训练过程
            X_train,X_test,y_train,y_test = train_test_split(X,y,test_size=0.2,random_state=0)
            dtrain = xgb.DMatrix(X_train,y_train)
            num_rounds = 300
            plst = params.items()
            model = xgb.train(plst,dtrain,num_rounds)
            # 对测试集进行预测
            print(np.corrcoef(model.predict(xgb.DMatrix(Xtest)),ytest))
            # 显示重要特征
            plot_importance(model)
            plt.show()
            #记录model模型
            savePath=os.path.join(self.path,'xgb001.model')
            model.save_model(savePath)
            pass
               

        pass

    #----------------------------------------------------------------------
    def mytest(self,startDate,endDate):
        pass

########################################################################