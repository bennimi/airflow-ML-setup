import psycopg2, os
from urllib.parse import urlparse


db_uri = urlparse(os.getenv("DATABASE_URL", "sqlite://"))

user = db_uri.username
password = db_uri.password
dbname = db_uri.path[1:]
host = db_uri.hostname
port = db_uri.port

conn = psycopg2.connect(host=host, port=port, user=user, password=password, dbname=dbname)
cursor = conn.cursor()
cursor.execute("INSERT INTO public.connection(conn_id,conn_type,host,login,port,is_encrypted,is_extra_encrypted) VALUES ('{}','{}','{}','{}',{},{},{});".format(
                                                                                    'postgres_hook','postgres','postgres','airflow',5432,False,False)
    )
conn.commit() 
cursor.close()
conn.close()
