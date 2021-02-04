# set base image
FROM puckel/docker-airflow:1.10.9

# dags folder 
COPY . /usr/local/airflow 
#COPY ./requirements.txt /usr/local/requirements.txt

# add workdir 
WORKDIR /usr/local/airflow

# add folder of later mount point
#RUN mkdir /usr/local/airflow/modeldata/
# add user to access shared volume
#RUN useradd voluser
# grant permisson
#RUN chown voluser:voluser /usr/local/modeldata

# add packages
RUN pip install -r requirements.txt   #sklearn gensim nltk num2words spacy 

# add pythonpath
ENV PYTHONPATH "${PYTHONPATH}:/usr/local/airflow/dags"
ENV PYTHONPATH "${PYTHONPATH}:/usr/local/airflow/.local/lib/python3.7/site-packages"







