# -*- coding: utf-8 -*-
"""

@author: bennimi
"""

import pickle
from sklearn.base import BaseEstimator, TransformerMixin

class TextTransformer(BaseEstimator, TransformerMixin):
    def __init__(self, key):
        self.key = key

    def fit(self, X, y=None, *parg, **kwarg):
        return self

    def transform(self, X):
        return X[self.key]

class CustomUnpickler(pickle.Unpickler):
    """
    found on:
    https://stackoverflow.com/questions/27732354/unable-to-load-files-using-pickle-and-multiple-modules
    """
    def find_class(self, module, name):
        if name == 'TextTransformer':
            #from app.helper_functions import TextTransformer
            return TextTransformer
        return super().find_class(module, name)
    
def nltk_downloads():
    import nltk
    for dependency in ("wordnet", "stopwords", "brown", "names","punkt"):
        nltk.download(dependency)
    
# from airflow.hooks.postgres_hook import PostgresHook

# def create_cursor():
#     pg_hook = PostgresHook(postgres_conn_id='postgres_hook')
#     connection = pg_hook.get_conn()
#     cursor = connection.cursor()
#     return cursor,connection
