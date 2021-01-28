CREATE DATABASE model_db
    WITH 
    OWNER = airflow
    ENCODING = 'UTF8'
    LC_COLLATE = 'en_US.utf8'
    LC_CTYPE = 'en_US.utf8'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1;

\connect model_db

CREATE TABLE public.tweets(
    id SERIAL PRIMARY KEY,
    timestamp_col TIMESTAMP,
    tweets_org  character varying(500),
    tweets_cld  character varying(500),
    predictions smallint NOT NULL
    );
    
CREATE TABLE public.models(
    id SERIAL PRIMARY KEY,
    timestamp_col TIMESTAMP,
    model BYTEA, 
    model_name  character varying(100),
    model_acc REAL,
    model_f1 REAL
    );