from airflow import settings
from airflow.models import Connection
conn = Connection(
        conn_id='postgres_hook',
        conn_type='postgres',
        host='postgres',
        login='airflow',
     
        port=5432) #create a connection object
session = settings.Session() # get the session
session.add(conn)
session.commit() # it will insert the connection object programmatically.