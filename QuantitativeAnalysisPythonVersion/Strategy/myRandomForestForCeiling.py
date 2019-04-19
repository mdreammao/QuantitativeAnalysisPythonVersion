from Config.myConstant import *
from Config.myConfig import *
import pandas as pd
from sklearn.ensemble import RandomForestClassifier  
from sklearn import metrics
from sklearn.model_selection import KFold
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
        mydata=store.select('ceiling')
        #mydata=mydata[mydata['return10m']!=0]
        mydata=mydata.reset_index(drop=True)
        store.close()
        return mydata
    
    #--------------------------------------------------------------------
    def __myClassifier(self,X,y,x_columns,target):
        kf=KFold(n_splits=5)
        for train_index,test_index in kf.split(X):
            X_train,X_test=X.loc[train_index,:],X.loc[test_index,:]
            y_train,y_test=y.loc[train_index],y.loc[test_index]
            rf0 = RandomForestClassifier(oob_score=True, random_state=10,n_estimators=500,class_weight= {-1:1,0:0.01,1:1},n_jobs=8)
            rf0.fit(X_train,y_train)
            y_predprob = rf0.predict_proba(X_test)[:,1] 
            y_pred = rf0.predict(X_test)
            fpr, tpr, thresholds = metrics.roc_curve(y_test,y_predprob, pos_label=1)
            auc=metrics.auc(fpr, tpr)
            cm=metrics.confusion_matrix(y_test, y_pred)#混淆矩阵
            featurImportances=sorted(zip(map(lambda x: round(x, 4), rf0.feature_importances_), x_columns),reverse=True)
            print("特征重要性:",featurImportances)
            print("泛化能力:",rf0.oob_score_) 
            #print("AUC:",metrics.roc_auc_score(y_test,y_predprob))
            print("准确率:",metrics.accuracy_score(y_test,y_pred))
            #print("召回率:",metrics.recall_score(y_test,y_pred))
            #print("F测度:",metrics.f1_score(y_test, y_pred))
            print("混淆矩阵:",cm)
        pass
           
    #--------------------------------------------------------------------
    def myRandomForest(self,target):
        train=self.__getMyData()
        
        featureNames=['time','increase5m', 'increase1m', 
       'ceilingYesterday', 'ceilingYesterday2', 'ceilingIn5Days',
       'increaseInDay']
        x_columns=[x for x in train.columns if x in featureNames]
        X=train[x_columns]
        y=train[target]
        self.__myClassifier(X,y,x_columns,target)
        #joblib.dump(rf0, "rfTrainModel.m")#保存模型


        
    #----------------------------------------------------------------------
    #----------------------------------------------------------------------
########################################################################