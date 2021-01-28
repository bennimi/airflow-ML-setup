# -*- coding: utf-8 -*-
"""
Created on Sun Dec 27 21:09:40 2020

@author: Benny Mü

info: Postgres-hook!
"""

from airflow.hooks.postgres_hook import PostgresHook

def create_cursor():
    pg_hook = PostgresHook(postgres_conn_id='postgres_hook')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    return cursor,connection