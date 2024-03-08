import psycopg2

try:
    connection = psycopg2.connect(
        dbname='test_task_doc_db',
        host='localhost',
        user='db_user',
        password='password111',
    )
    print("Подключение установлено")
    cursor = connection.cursor()

    create_tables = '''CREATE TABLE IF NOT EXISTS public.data
    (
        object character varying(50) COLLATE pg_catalog."default" NOT NULL,
        status integer,
        level integer,
        parent character varying COLLATE pg_catalog."default",
        owner character varying(14) COLLATE pg_catalog."default",
        CONSTRAINT data_pkey PRIMARY KEY (object)
    );
    CREATE TABLE IF NOT EXISTS public.documents
    (
        doc_id character varying COLLATE pg_catalog."default" NOT NULL,
        recieved_at timestamp without time zone,
        document_type character varying COLLATE pg_catalog."default",
        document_data jsonb,
        processed_at timestamp without time zone,
        CONSTRAINT documents_pkey PRIMARY KEY (doc_id)
    )
    '''
    cursor.execute(create_tables)
    connection.commit()
    print("Таблицы успешно созданы!")
except (Exception, psycopg2.Error) as error:
    print("Ошибка при работе с PostgreSQL", error)
finally:
    if connection:
        cursor.close()
        connection.close()
        print("Соединение с PostgreSQL закрыто")