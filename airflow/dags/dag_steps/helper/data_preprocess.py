# -*- coding: utf-8 -*-
"""
Created on Sat Dec 26 21:09:23 2020

@author: Bennmi

info: - data pre-prepare before load to postgres
"""

import TextProcessorModule as tp  # import text tools
import pandas as pd
import random,sys


#fpath = r'C:/Virtual-Environments/docker-shared/airflow-setup/datasets/'
train_path = sys.argv[1]
df_path = sys.argv[2]

df = pd.read_csv(train_path+'training.csv')
#df.columns

pipe_init = tp.Preprocess_Pipeline()
pipe_init.PrePreProcessor(letters_only=False)
#pipe_init.fit()

tweet_clean = pipe_init.transform(df.text)

rnd_num = random.randint(0, len(df.text))
print(df.text[rnd_num],"\n\n", tweet_clean[rnd_num])


df = pd.concat([df.target,tweet_clean], axis=1)
df.dropna(inplace=True)

df = df.sample(50000)

df.to_csv(df_path,index=None,header=False)
