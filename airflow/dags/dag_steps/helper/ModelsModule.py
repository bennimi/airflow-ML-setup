# -*- coding: utf-8 -*-
"""

@author: bennimi

Info: ML preperation and models
"""

from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline, FeatureUnion
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.decomposition import TruncatedSVD
from sklearn.ensemble import RandomForestClassifier,AdaBoostClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.svm import SVC
from sklearn.model_selection import GridSearchCV,RandomizedSearchCV
#from xgboost import XGBClassifier
from sklearn.metrics import accuracy_score,recall_score, precision_score, classification_report, confusion_matrix
from sklearn.utils.fixes import loguniform
import pandas as pd
import numpy as np
import pickle, sys, time

## use multiple classifiers in one pipeline 
#https://stackoverflow.com/questions/23045318/scikit-grid-search-over-multiple-classifiers


class TextTransformer(BaseEstimator, TransformerMixin):
    def __init__(self, key):
        self.key = key

    def fit(self, X, y=None, *parg, **kwarg):
        return self

    def transform(self, X):
        return X[self.key]


    
# path = "C:/Virtual-Environments/docker-shared/airflow-setup/datasets/cleaned/"
# df_name = "prepared_1.csv"
# model_name = 'svc'
# #sys.path.append("C:/Virtual-Environments/docker-shared/airflow-setup/scripts")

# df = pd.read_csv(path+df_name,names = ['target','tweet'])
# df.dropna(inplace=True)
# #df = df.sample(1000)
# X_train, X_test, y_train, y_test = train_test_split(df[['tweet']], 
#                                                     df['target'],
#                                                     test_size=0.20)   


# feature_pipeline =  Pipeline([
#         ('transformer', TextTransformer(key='tweet')),
#         ('tfidf', TfidfVectorizer(ngram_range=(1,1))),
#         ('svd', TruncatedSVD(algorithm='randomized', n_components=300))
#         ])

# feature_pipeline.fit(X_train)
# #pickle.dump(feature_pipeline, open(path+"feature_pipeline_fitted.pkl", 'wb'))

# X_train_transformed = feature_pipeline.transform(X_train)
# X_test_transformed = feature_pipeline.transform(X_test)

# model = Pipeline([('features', feature_pipeline),
#                   ('SVC',SVC())
#                   ])

# parameter_grid = {'kernel':['linear','rbf'],
#                       'C':loguniform(1e-1, 1e2), 
#                       'gamma':loguniform(1e-3, 1e0)}

# model = RandomizedSearchCV(SVC(), parameter_grid, n_iter = 10, cv=5,n_jobs = -1)

# model.fit(X_train_transformed, y_train)
# preds = model.predict(X_test_transformed)


def rdf_model(feature_pipeline):
    rdf_pipeline = Pipeline([('features', feature_pipeline),
                     ('rdf',RandomForestClassifier())
                     ])
    
    parameter_grid = {'rdf__bootstrap': [True, False],
                      'rdf__criterion': ['gini', 'entropy'],
                      'rdf__max_depth': [10, 20, 30, 40, 50, 60, 70, 80, 90, 100, None],
                      'rdf__max_features': ['auto', 'sqrt'],
                      'rdf__min_samples_leaf': [1, 2, 4],
                      'rdf__min_samples_split': [2, 5, 10],
                      'rdf__n_estimators': [10,100,200, 400, 600, 800, 1000]}
    rdf_pipeline = RandomizedSearchCV(rdf_pipeline, parameter_grid, n_iter = 10, cv = 5, n_jobs = -1)    
    return rdf_pipeline
    
def logreg_model(feature_pipeline):
    logreg_pipeline = Pipeline([('features', feature_pipeline),
                      ('LogisticRegression',LogisticRegression())
                      ])
    
    parameter_grid = {'LogisticRegression__penalty' : ['none','l1', 'l2'],
                      'LogisticRegression__C' : np.logspace(-4, 4, 20),
                      'LogisticRegression__solver' : ['newton-cg', 'lbfgs', 'liblinear', 'saga']}
    logreg_pipeline = RandomizedSearchCV(logreg_pipeline, parameter_grid, n_iter = 10, cv=5,n_jobs = -1)
    return logreg_pipeline

def svc_model(feature_pipeline):
    svc_pipeline = Pipeline([('features', feature_pipeline),
                      ('SVC',SVC())
                      ])

    parameter_grid = {'SVC__kernel':['linear','rbf'],
                      'SVC__C':loguniform(1e-1, 1e2), 
                      'SVC__gamma':loguniform(1e-3, 1e0)}
    svc_pipeline = RandomizedSearchCV(svc_pipeline, parameter_grid, n_iter = 10, cv=5,n_jobs = -1)
    return svc_pipeline


# =============================================================================
# def selected_model():
#     if model_name == 'rdf': model = rdf_model(feature_pipeline)
#     if model_name == 'logreg': model = logreg_model(feature_pipeline)
#     if model_name == 'svc': model = svc_model(feature_pipeline)
#      
#     model.fit(X_train, y_train)
#     preds = model.predict(X_test)
# 
#     pickle.dump(model, open(path+model_name+".pkl", 'wb'))
#     
#     model = pickle.load(open(path+model_name+".pkl", 'rb'))
#     
#     #model.best_score_    
#     print(model.best_params_)
#     print ("Accuracy:", round(accuracy_score(y_test, preds),4))
#     print ("Precision:", round(precision_score(y_test, preds,labels=[0,4],pos_label=4),4))
#     print('Recall:',round(recall_score(y_test,preds,labels=[0,4],pos_label=4),4))
#     #print (classification_report(y_test, preds))
# =============================================================================


if __name__ == '__main__':
    pass    
    # model_name = sys.argv[1]
    # df_path = sys.argv[2]
    # model_path = sys.argv[3]