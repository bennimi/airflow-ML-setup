# -*- coding: utf-8 -*-
"""
Created on Sun Dec 27 20:47:54 2020

@author: Benny Mü

info: clean data for model training
"""


import TextProcessorModule as tp
import pandas as pd

#import Git_Repo_Nudge.TextProcessor as tp
#df = pd.read_csv(r"C:\Virtual-Environments\docker-shared\airflow-setup\datasets\cleaned\cleaned.csv", header=None)
#df = df.sample(100)

def create_clean_1(in_path,name):
    df = pd.read_csv(in_path, header=None)
    clean_1 =  tp.Preprocess_Pipeline()
    clean_1.Tokenizer(stopword_remove=True,
                      length_remove=2,
                      lemmatizer=True,
                      num_to_word=True,
                      to_sentence=True)
    df_temp = clean_1.transform(df.iloc[:,1])
    df_temp = pd.concat([df.iloc[:,0],df_temp], axis=1)
    df_temp.to_csv(in_path+"prepared_1.csv",index=None,header=False)


def create_clean_2(in_path,name):
    df = pd.read_csv(in_path, header=None)
    clean_2 =  tp.Preprocess_Pipeline()
    clean_2.Tokenizer(stopword_remove=False,
                      to_sentence=True)
    df_temp = clean_2.transform(df.iloc[:,1])
    df_temp = pd.concat([df.iloc[:,0],df_temp], axis=1)
    df_temp.to_csv(in_path+"prepared_2.csv",index=None,header=False)



#clean_3 =  tp.Preprocess_Pipeline()
#clean_4 =  tp.Preprocess_Pipeline()


