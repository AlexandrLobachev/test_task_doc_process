import psycopg2


"""Собираем все документы."""
try:
    connection = psycopg2.connect(
        dbname='test_task_doc_db',
        host='localhost',
        user='db_user',
        password='password111',
    )
    print("Подключение установлено")
    cursor = connection.cursor()

    get = '''
    SELECT * FROM public.documents
    WHERE document_type = 'transfer_document'
    ORDER BY recieved_at ASC
    '''
    cursor.execute(get)
    documents = cursor.fetchall()
except (Exception, psycopg2.Error) as error:
    print("Ошибка при работе с PostgreSQL", error)
finally:
    if connection:
        cursor.close()
        connection.close()
        print("Соединение с PostgreSQL закрыто")


def get_data(objects):
    """Возваращает объекты data c parent из списка document_data.objects."""

    try:
        connection = psycopg2.connect(
            dbname='test_task_doc_db',
            host='localhost',
            user='db_user',
            password='password111',
        )
        print("Подключение установлено")
        cursor = connection.cursor()

        get = '''
        SELECT * FROM public.data
        WHERE data.parent IN %s
        '''
        cursor.execute(get, (objects,))
        data = cursor.fetchall()
        return data
    except (Exception, psycopg2.Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("Соединение с PostgreSQL закрыто")


def update_data(objects):
    """Обновляет данные в БД. Еще не доделана."""
    try:
        connection = psycopg2.connect(
            dbname='test_task_doc_db',
            host='localhost',
            user='db_user',
            password='password111',
        )
        print("Подключение установлено")
        cursor = connection.cursor()

        get = '''
        UPDATE public.data SET %s
        WHERE data.object IN %s
        '''
        cursor.execute(get, (objects,))
        data = cursor.fetchall()
        #print(data)
        return data
    except (Exception, psycopg2.Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("Соединение с PostgreSQL закрыто")

def parece_document(document):
    """Парсит полученный документ."""
    doc_id = document[0]
    data = document[3]
    objects = tuple(data.get('objects'))
    operation_details = data.get('operation_details')
    return doc_id, objects, operation_details


def felter_data(data, operation_details):
    """Возвращает объекты data, у которых поля old совпадают с условием document."""
    filtred_data = []
    old_values = [detail.get('old') for detail in operation_details.values()]
    for doc in data:
        if set(old_values).issubset(set(doc)):
            filtred_data.append(doc)
    return filtred_data


def main():
    """Основная логика скрипта."""
    for doc in documents:
        doc_id, objects, operation_details = parece_document(doc)
        data = get_data(objects)
        feltered_data = felter_data(data, operation_details)
        print(feltered_data, len(feltered_data))

        # update_data(new_data, feltered_data)


if __name__ == '__main__':
    main()
