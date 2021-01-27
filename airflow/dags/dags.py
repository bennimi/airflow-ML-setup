# -*- coding: utf-8 -*-
"""

@author: Bennimi

info: init DAG with corresponding fuctions for airflow
"""

# 1.1 import modules
from airflow.models import DAG
from airflow.utils.dates import days_ago,timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.trigger_rule import TriggerRule

from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline, FeatureUnion
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import TruncatedSVD
from sklearn.metrics import accuracy_score,recall_score, precision_score, classification_report, confusion_matrix

import csv, os, glob, sys, random, pickle
import pandas as pd

import TextProcessorModule as tp
from TextProcessorModule import TextTransformer

from ModelsModule import svc_model, rdf_model, logreg_model

#from datetime import datetime, timedelta


# 2.1 set variables 
### create airflow variables
###### https://www.youtube.com/watch?v=bHQ7nzn0j6k


script_path = r"/usr/local/scripts/"
df_path = r"/usr/local/datasets/"

models = {'rdf':rdf_model,'logreg':logreg_model,'svc':svc_model}

# 2.2 init dict-args
args ={'owner':'bennimi','start_date':days_ago(1)}

# 2.3 init dag
dag = DAG(dag_id='ml_pipe',default_args=args,schedule_interval=None)

# 3 define functions
# 3.0 helper funtions
def create_cursor():
    pg_hook = PostgresHook(postgres_conn_id='postgres_hook')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    return cursor,connection

# 3.1 create
""" sensors"""

def check_num_files(**context):
    all_files = glob.glob("/usr/local/airflow/train/*.csv")
    if not len(all_files) == 1:     
        raise ValueError('Input check found more than 1 file: \nplease check "/usr/local/airflow/train/" ')       
    context['ti'].xcom_push(key='all_files',value=all_files[0])
        
# 3.2 check import file
def check_import(**context):
    in_file = context['ti'].xcom_pull(key='all_files')
    df = pd.read_csv(in_file, index_col=None, header=0)
    if not 'tweet' and 'text' in df.columns:  
        raise ValueError('Input must contain columns [target,text]')
    df = df.filter(['target', 'text'],axis=1)
    df.to_csv(df_path + "checked.csv",index=None,header=False)
    context['ti'].xcom_push(key='checked',value=df_path + "checked.csv")

def pre_clean(**context):
    in_file = context['ti'].xcom_pull(key='checked')
    df = pd.read_csv(in_file, index_col=None, header=None)
    
    pipe_init = tp.Preprocess_Pipeline()
    pipe_init.PrePreProcessor(letters_only=False)
    tweet_clean = pipe_init.transform(df[1])
    df = pd.concat([df[0],tweet_clean], axis=1)
    df = df[df[1].str.strip().astype(bool)] #delete "empty" rows  
    df = df.sample(1000)     
    df.to_csv(df_path+'cleaned.csv',index=None,header=False)    
    context['ti'].xcom_push(key='cleaned',value=df_path + "cleaned.csv")

def feature_pipeline(**context):    
    feature_pipeline =  Pipeline([
            ('transformer', TextTransformer(key='tweet')),
            ('tfidf', TfidfVectorizer(ngram_range=(1,3))),
            ('svd', TruncatedSVD(algorithm='randomized', n_components=300))])
    
    pickle.dump(feature_pipeline, open(df_path+"feature_pipeline.pkl", 'wb'))
    context['ti'].xcom_push(key='feature_pipeline',value=df_path+"feature_pipeline.pkl")

def data_prepare(csv_id,**context):
    #cursor,conn = create_cursor()
    #data_query = pd.read_sql_query("""select * from twitter_table;""",conn)
    #df = pd.DataFrame(data_query)
    in_file = context['ti'].xcom_pull(key='cleaned')
    df = pd.read_csv(in_file, index_col=None, header=None)
    if csv_id == 1:        
        clean =  tp.Preprocess_Pipeline()
        clean.Tokenizer(stopword_remove=True,
                          length_remove=2,
                          lemmatizer=True,
                          num_to_word=True,
                          to_sentence=True)
    
    if csv_id == 2:
        clean =  tp.Preprocess_Pipeline()
        clean.Tokenizer(stopword_remove=False,
                      to_sentence=True)
        
    df_temp = clean.transform(df.iloc[:,1])
    df_temp = pd.concat([df.iloc[:,0],df_temp], axis=1)
    df_temp = df_temp[df_temp[1].str.strip().astype(bool)] #delete "empty" rows        
    df_temp.to_csv(df_path+"prepared_{}.csv".format(csv_id),index=None,header=False)
    context['ti'].xcom_push(key='prepared_{}'.format(csv_id),value=df_path + "prepared_{}.csv".format(csv_id))


def data_split(csv_id,**context):
    in_file = context['ti'].xcom_pull(key='prepared_{}'.format(csv_id))
    df = pd.read_csv(in_file, index_col=None, names=['target','tweet'])
    X_train, X_test, y_train, y_test = train_test_split(df[['tweet']], 
                                                    df['target'],
                                                    test_size=0.20)  
    for k_elm,v_elm in {'X_train':X_train, 'X_test':X_test, 'y_train': y_train,'y_test': y_test}.items():
        pickle.dump(v_elm, open(df_path+"{}_{}.pkl".format(k_elm,csv_id), 'wb'))
        context['ti'].xcom_push(key='{}_{}'.format(k_elm,csv_id),value=df_path + "{}_{}.pkl".format(k_elm,csv_id))

        
def train_model(model_id,model,csv_id,**context):  
    feature_pipeline = pickle.load(open(context['ti'].xcom_pull(key='feature_pipeline'),'rb'))
    data_dict = {'X_train':None, 'X_test':None, 'y_train': None,'y_test': None}
    for elm in data_dict:
        data_dict[elm] = pickle.load(open(context['ti'].xcom_pull(key='{}_{}'.format(elm,csv_id)),'rb'))
    
    model = model(feature_pipeline)
    # if model_id == 'logreg':    
    #     from ModelsModule import logreg_model
    #     model = logreg_model(feature_pipeline)   
    # if model_id == 'rdf':    
    #     from ModelsModule import rdf_model
    #     model = rdf_model(feature_pipeline)
    # if model_id == 'svc':    
    #     from ModelsModule import svc_model
    #     model = svc_model(feature_pipeline)
       
    model.fit(data_dict['X_train'], data_dict['y_train'])
    preds = model.predict(data_dict['X_test'])     
    
    pickle.dump(model, open(df_path+"{}_{}.pkl".format(model_id,csv_id), 'wb'))
    
    return model.best_params_, ("Accuracy:", round(accuracy_score(data_dict['y_test'], preds),4)),\
        ("Precision:", round(precision_score(data_dict['y_test'], preds,labels=[0,4],pos_label=4),4)),\
        ('Recall:',round(recall_score(data_dict['y_test'],preds,labels=[0,4],pos_label=4),4))
    
create_table_sql = """CREATE TABLE IF NOT EXISTS twitter_table (
                     id SERIAL PRIMARY KEY,
                     date DATE,
                     target INT,
                     tweet VARCHAR(280));
                     TRUNCATE TABLE twitter_table;
                     """

def insert_data(csv_id,**context):
    cursor,connection = create_cursor()
    in_file = context['ti'].xcom_pull(key='prepared_{}'.format(csv_id))
    with open(in_file,encoding='utf8') as csv_file:
        csv_data = csv.reader(csv_file)
        for row in csv_data:
            cursor.execute("""
                    INSERT INTO twitter_table_{} (date,target,tweet) VALUES
                        (current_date,{},'{}');""".format(csv_id,row[0],row[-1])
                        )
    connection.commit()

def query_table():
    cursor,conn = create_cursor()
    cursor.execute(""" select count(id) from twitter_table;
                   """)
    print("Datarows: ",cursor.fetchall())
    #data_query = pd.read_sql_query("""select * from twitter_table;""",conn)
                      
   
# 4 set up dag
with dag:
    
    run_check_num_file = PythonOperator(
        task_id = 'check_num_file',
        python_callable = check_num_files,
        provide_context = True)

    run_check_import = PythonOperator(
        task_id = 'check_import',
        python_callable = check_import,
        provide_context = True,
        retries = 2,
        retry_delay=timedelta(seconds=1))
    
    run_pre_clean = PythonOperator(
        task_id = 'pre_clean',
        python_callable = pre_clean,
        provide_context = True,
        retries = 2,
        retry_delay=timedelta(seconds=1))
    
    run_feature_pipeline = PythonOperator(
        task_id = 'feature_pipeline',
        python_callable = feature_pipeline,
        provide_context = True,)  
    
    run_data_prepare_1 = PythonOperator(
        task_id = 'data_prepare_1',
        python_callable = data_prepare,
        provide_context = True,
        op_kwargs={'csv_id': 1})
    
    run_data_prepare_2 = PythonOperator(
        task_id = 'data_prepare_2',
        python_callable = data_prepare,
        provide_context = True,
        op_kwargs={'csv_id': 2})
    
    run_data_split_1 = PythonOperator(
        task_id = 'data_split_1',
        python_callable = data_split,
        provide_context = True,
        op_kwargs={'csv_id': 1})
    
    run_data_split_2 = PythonOperator(
        task_id = 'data_split_2',
        python_callable = data_split,
        provide_context = True,
        op_kwargs={'csv_id': 2})
        
    # run_train_model = PythonOperator(
    #     task_id = 'train_model',
    #     python_callable = train_model,
    #     provide_context = True,
    #     op_kwargs={'model_id':'logreg','csv_id': 1},
    #     trigger_rule = TriggerRule.ALL_SUCCESS)      
    
    run_dummy = DummyOperator(
            task_id='dummy',
            trigger_rule = TriggerRule.ALL_SUCCESS)
    
    # run_create_table = PostgresOperator(
    #     task_id='create_table',
    #     postgres_conn_id='postgres_hook',
    #     sql = create_table_sql)    
    
    # run_train_model = BashOperator(
    #     task_id = 'rdf',
    #     bash_command = "python {0}models.py {1} {2} {3}".format(script_path,
    #                                                         ),
    #     provide_context = True)
    

    
run_check_num_file >> run_check_import >> run_pre_clean >> run_feature_pipeline >>\
[run_data_prepare_1,run_data_prepare_2]
run_data_prepare_1 >> run_data_split_1
run_data_prepare_2 >> run_data_split_2
[run_data_split_1,run_data_split_2] 

for csv_id, upstream_dag in enumerate([run_data_split_1,run_data_split_2]):
    for model_id,model in models.items():
        run_model = PythonOperator(
            task_id = 'run_model_{}_{}'.format(model_id,csv_id),
            python_callable = train_model,
            provide_context = True,
            op_kwargs={'model_id':model_id,'model':model,'csv_id': csv_id+1},
            dag=dag
            )
        upstream_dag >> run_model
        run_model >> run_dummy


