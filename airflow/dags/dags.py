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
from sklearn.metrics import accuracy_score,recall_score, precision_score,f1_score, classification_report, confusion_matrix

import csv, os, glob, sys, random, pickle
from io import StringIO
import pandas as pd

import dag_setup.helper.TextProcessorModule as tp
from dag_setup.helper.helper_functions import TextTransformer
from dag_setup.ModelsModule import svc_model, rdf_model, logreg_model, feature_pipeline
#from dag_setup.postgres_hook_config import pg_hook # discarded


#from datetime import datetime, timedelta


# 2.1 set variables 
### create airflow variables --> documentation airflow howto

# define paths
script_path = r"/usr/local/scripts/"
df_path = r"/usr/local/datasets/"

# ML setup
test_size = 0.2
feature_pipeline_specs =  feature_pipeline(key="tweet",n_grams=(1,2),n_comp=150)
models = {'rdf':rdf_model,'logreg':logreg_model,'svc':svc_model} # extensible, models just need to follow structure from ModelsModule
n_samples = 100

# DB hook
pg_hook = PostgresHook(postgres_conn_id='postgres_hook')

# 2.2 init dict-args
args ={'owner':'bennimi','start_date':days_ago(1)}

# 2.3 init dag
dag = DAG(dag_id='ml_pipe',default_args=args,schedule_interval=None)

# 3 define functions
# 3.1 create
""" sensors"""

def check_num_files(**context):
    train_file = glob.glob(df_path+"train/*.csv")
    if not len(train_file) == 1:     
        raise ValueError('Input check found more than 1 file: \nplease check "/usr/local/airflow/train/" ')       
    context['ti'].xcom_push(key='train_file',value=train_file[0])
        
# 3.2 check import file
def check_import(**context):
    in_file = context['ti'].xcom_pull(key='train_file')
    df = pd.read_csv(in_file, index_col=None, header=0)
    if not 'tweet' and 'text' in df.columns:  
        raise ValueError('Input must contain columns labelled as [target,text]')
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
    df = df.sample(n_samples)     
    df.to_csv(df_path+'cleaned.csv',index=None,header=False)    
    context['ti'].xcom_push(key='cleaned',value=df_path + "cleaned.csv")

# put in setup in config at the beginning and call from data_clean
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
                                                    test_size=test_size)  
    for k_elm,v_elm in {'X_train':X_train, 'X_test':X_test, 'y_train': y_train,'y_test': y_test}.items():
        pickle.dump(v_elm, open(df_path+"{}_{}.pkl".format(k_elm,csv_id), 'wb'))
        context['ti'].xcom_push(key='{}_{}'.format(k_elm,csv_id),value=df_path + "{}_{}.pkl".format(k_elm,csv_id))


# makes sense to store fitted pipe and transformed data instead of only the pipe specs.. 
# How to store only parts of a pipe?! ..question stackowerflow pending  --> if not possible, apply workaround.. 
def feature_pipeline_fit(**context):       
    pickle.dump(feature_pipeline_specs, open(df_path+"feature_pipeline.pkl", 'wb'))
    context['ti'].xcom_push(key='feature_pipeline',value=df_path+"feature_pipeline.pkl")

        
def train_model(model_id,model,csv_id,**context):  
    feature_pipeline = pickle.load(open(context['ti'].xcom_pull(key='feature_pipeline'),'rb'))
    data_dict = {'X_train':None, 'X_test':None, 'y_train': None,'y_test': None}
    for elm in data_dict:
        data_dict[elm] = pickle.load(open(context['ti'].xcom_pull(key='{}_{}'.format(elm,csv_id)),'rb'))
    
    model = model(feature_pipeline)
  
    model.fit(data_dict['X_train'], data_dict['y_train'])
    preds = model.predict(data_dict['X_test'])     
    
    pickle.dump(model, open(df_path+"{}_{}.pkl".format(model_id,csv_id), 'wb'))
    
    # model
    context['ti'].xcom_push(key='model-{}_{}'.format(model_id,csv_id),value=df_path + "{}_{}".format(model_id,csv_id))
    
    # scores 
    context['ti'].xcom_push(key='scores-{}_{}'.format(model_id,csv_id), value="{},{}".format(
                                                round(accuracy_score(data_dict['y_test'], preds),4),
                                                round(f1_score(data_dict['y_test'],preds,labels=[0,4],pos_label=4),4)
                                                )
                            )
    
    return model.best_params_, ("Accuracy:", round(accuracy_score(data_dict['y_test'], preds),4)),\
        ("Precision:", round(precision_score(data_dict['y_test'], preds,labels=[0,4],pos_label=4),4)),\
        ('f1:',round(f1_score(data_dict['y_test'],preds,labels=[0,4],pos_label=4),4))
    
def model_insert(model_id,csv_id,**context):
    model_name = context['ti'].xcom_pull(key='model-{}_{}'.format(model_id,csv_id))
    model = pickle.load(open(model_name+'.pkl','rb'))
    scores = context['ti'].xcom_pull(key='scores-{}_{}'.format(model_id,csv_id))
    scores = scores.split(',')
    dts_insert = "INSERT INTO TABLE models (model,model_name,model_acc,model_f1) VALUES (%s, %s, %s, %s);"
    pg_hook.run(dts_insert, parameters=(model, model_name,scores[0],scores[1]))


## discarded, using native PostgresHook instead
# =============================================================================
# def model_insert(model_id,csv_id,**context):
#     model_name = context['ti'].xcom_pull(key='model-{}_{}'.format(model_id,csv_id))
#     model = pickle.load(open(model_name+'.pkl','rb'))
#     scores = context['ti'].xcom_pull(key='scores-{}_{}'.format(model_id,csv_id))
#     scores = scores.split(',')
#     pg_conn = pg_hook()
#     pg_cur = pg_conn.cursor()
#     pg_cur.execute("INSERT INTO TABLE models (model,model_name,model_acc,model_f1) VALUES ('{}', '{}', '{}', '{}')".format(model,model_name,scores[0],scores[1]))
#     # pg_cur.close()
#     pg_conn.close()
# =============================================================================

def tweets_insert():
    pg_conn = pg_hook()
    pg_cur = pg_conn.cursor()
    f = open('', 'r')
    pg_cur.copy_from(f, 'tablename', columns=())
    
def query_table():
    pg_conn = pg_hook()
    pg_cur = pg_conn.cursor()
    pg_cur.execute(""" select count(id) from twitter_table;
                   """)
    print("Datarows: ",pg_cur.fetchone())
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
        insert_model = PythonOperator(
            task_id = 'insert_model_{}_{}'.format(model_id,csv_id),
            python_callable = model_insert,
            provide_context = True,
            op_kwargs={'model_id':model_id,'csv_id': csv_id+1},
            dag=dag
            )
        upstream_dag >> run_model >> insert_model
        insert_model >> run_dummy

