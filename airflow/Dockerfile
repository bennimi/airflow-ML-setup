# set base image
FROM puckel/docker-airflow:1.10.9

# dags folder 
COPY . /usr/local/airflow 
#COPY ./requirements.txt /usr/local/requirements.txt

# add workdir 
WORKDIR /usr/local/airflow

# add pythonpath
ENV PYTHONPATH "${PYTHONPATH}:/usr/local/airflow/dags"

# add packages
RUN pip install -r requirements.txt   #sklearn gensim nltk num2words spacy 





