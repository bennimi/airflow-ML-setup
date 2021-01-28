# 1 - set base image
FROM puckel/docker-airflow:1.10.8

# 2 - add dataset
#COPY ./datasets/train/ train/  

# add workdir
COPY . /usr/local/  
#COPY ./requirements.txt /usr/local/requirements.txt

WORKDIR /usr/local/

# 3 - add pythonpath
ENV PYTHONPATH "${PYTHONPATH}:/usr/local/airflow/dags"

# 4 - add packages
RUN pip install -r requirements.txt   #sklearn gensim nltk num2words spacy 





