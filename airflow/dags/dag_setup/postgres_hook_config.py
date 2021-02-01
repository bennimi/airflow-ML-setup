def pg_hook():    
    import os,psycopg2
    from urllib.parse import urlparse
    db_uri = urlparse(os.getenv("DATABASE_URL", "sqlite://"))
    
    login = db_uri.username
    password = db_uri.password
    dbname = db_uri.path[1:]
    host = db_uri.hostname
    port = db_uri.port
        
    return psycopg2.connect(host=host, port=port, user=login, password=password, dbname=dbname)

# cur = conn.cursor()
# cur.execute("INSERT INTO connection (conn_id, conn_type, host, schema, login, password, port, is_encrypted, is_extra_encrypted) VALUES ('{}', '{}', '{}', '{}', '{}', '{}', {}, '{}', '{}')".format(
#     conn_id,conn_type,host,schema,login,password,port,False,False))
            
# conn.commit()
# conn.close()



class conn_setup:
    import os
    from urllib.parse import urlparse
    from airflow import settings
    from airflow.models import Connection

    """
    conn_id: set up in docker-compose as MODEL_DATABASE_CONN_NAME'
    or provide a name here, directly.
    """
    conn_id='postgres_hook'
    db_uri = urlparse(os.getenv("DATABASE_URL", "sqlite://"))
    
    login = db_uri.username
    password = db_uri.password
    schema = db_uri.path[1:]
    host = db_uri.hostname
    port = db_uri.port
    
    #schema=os.getenv("MODEL_DATABASE_NAME")
    conn_type = db_uri.scheme
    
    conn = Connection(
            conn_id=conn_id,
            conn_type=conn_type,
            host=host,
            login=login,
            password=password,
            schema=schema,
            port=port) #create a connection object
    session = settings.Session() # get the session
    session.add(conn)
    session.commit() # insert the connection object.
    
    def postgres_hook():
        from airflow.hooks.postgres_hook import PostgresHook
        return PostgresHook(postgres_conn_id=conn_setup.conn_id)




