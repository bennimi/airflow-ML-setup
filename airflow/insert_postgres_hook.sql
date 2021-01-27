
\connect airflow
INSERT INTO public.connection(
	conn_id, conn_type, host, login, port, is_encrypted, is_extra_encrypted)
	VALUES ('postgres_hook', 'postgres', 'postgres', 'airflow', 5432, false, false); 