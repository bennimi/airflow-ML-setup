# 1 - set base image
FROM puckel/docker-airflow:1.10.8

# 2 - add dataset
#COPY ./datasets/train/ train/  
#COPY ./postgres_hook_config.py /usr/local/airflow/ 
#COPY ./requirements.txt /usr/local/airflow/
COPY . /usr/local/

# 3 - add pythonpath
ENV PYTHONPATH "${PYTHONPATH}:/usr/local/dags"

# 4 - add packages
RUN pip install -r requirements.txt   #sklearn gensim nltk num2words spacy 


# 5 - command that runs when container starts

#CMD bash "sed -i 's+base_url = http://localhost:8080+base_url = http://localhost:8080$\nrbac = True+g' /usr/local/airflow/airflow.cfg"


