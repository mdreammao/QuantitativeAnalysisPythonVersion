from Config.myConstant import *
from Config.myConfig import *
import pandas as pd
from sklearn.ensemble import RandomForestClassifier  
from sklearn import metrics
from sklearn.externals import joblib

########################################################################
class myRandomForestForCeiling(object):
    """利用随机森林算法分类涨停情况"""
    #----------------------------------------------------------------------
    def __init__(self):
        self.__localFileStr=LocalFileAddress+"\\intermediateResult\\ceilingFeature.h5"
        
    #----------------------------------------------------------------------
    def __getMyData(self):
        store = pd.HDFStore(self.__localFileStr,'a')
        self.__mydata=store.select('ceiling')
        store.close()
    #--------------------------------------------------------------------
    def myRandomForest(self,target):
        self.__getMyData()
        train=self.__mydata
        featureNames=['time','increase5m', 'increase1m', 
       'ceilingYesterday', 'ceilingYesterday2', 'ceilingIn5Days',
       'increaseInDay']
        x_columns=[x for x in train.columns if x in featureNames]
        X=train[x_columns]
        y=train[target]
        rf0 = RandomForestClassifier(oob_score=True, random_state=10,n_estimators=500) 
        rf0.fit(X,y)
        y_predprob = rf0.predict_proba(X)[:,1] 
        y_pred = rf0.predict(X)
        fpr, tpr, thresholds = metrics.roc_curve(y,y_predprob, pos_label=1)
        auc=metrics.auc(fpr, tpr)
        cm=metrics.confusion_matrix(y, y_pred)#混淆矩阵
        featurImportances=sorted(zip(map(lambda x: round(x, 4), rf0.feature_importances_), x_columns),reverse=True)
        print("特征重要性:",featurImportances)
        print("泛化能力:",rf0.oob_score_) 
        print("AUC:",metrics.roc_auc_score(y,y_predprob))
        print("准确率:",metrics.accuracy_score(y,y_pred))
        print("召回率:",metrics.recall_score(y,y_pred))
        print("F测度:",metrics.f1_score(y, y_pred))
        joblib.dump(rf0, "rfTrainModel.m")#保存模型


        
    #----------------------------------------------------------------------
    #----------------------------------------------------------------------
########################################################################