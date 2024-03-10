import copy
import psycopg2


def get_document() -> tuple:
    """Возвращает не обработанный документ."""
    try:
        connection = psycopg2.connect(
            dbname='test_task_doc_db',
            host='localhost',
            user='db_user',
            password='password111',
        )
        cursor = connection.cursor()

        query = '''
        SELECT * FROM public.documents
        WHERE document_type = 'transfer_document'
        AND processed_at IS NULL
        ORDER BY recieved_at ASC
        '''
        cursor.execute(query)
        return cursor.fetchone()
    except (Exception, psycopg2.Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()


def get_data(objects: tuple) -> list:
    """Возваращает объекты data c parent из списка document_data.objects."""
    try:
        connection = psycopg2.connect(
            dbname='test_task_doc_db',
            host='localhost',
            user='db_user',
            password='password111',
        )
        cursor = connection.cursor()

        query = '''
        SELECT * FROM public.data
        WHERE data.parent IN %s
        '''
        cursor.execute(query, (objects,))
        data = cursor.fetchall()
        return data
    except (Exception, psycopg2.Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()


def update_data(query: str, values: dict):
    """Обновляет данные в БД."""
    try:
        connection = psycopg2.connect(
            dbname='test_task_doc_db',
            host='localhost',
            user='db_user',
            password='password111',
        )
        cursor = connection.cursor()

        cursor.executemany(query, (values,))
        connection.commit()

    except (Exception, psycopg2.Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()


def parce_document(document: tuple) -> tuple:
    """Парсит полученный документ."""
    doc_id = document[0]
    data = document[3]
    objects = tuple(data.get('objects'))
    operation_details = data.get('operation_details')
    old_values_dict = {k: v.get('old') for k, v in operation_details.items()}
    new_values_dict = {k: v.get('new') for k, v in operation_details.items()}
    return doc_id, objects, new_values_dict, old_values_dict


def check_data(data: list, values: dict) -> tuple:
    """Возвращает PK объектов, с подходящими параметрами old."""
    mapping = {
        'status': 1,
        'owner': 4,
    }
    not_filtered_data = copy.deepcopy(data)
    filtered_data = []
    for detail, val in values.items():
        filtered_data.clear()
        for doc in not_filtered_data:
           if doc[mapping[detail]] == val:
               filtered_data.append(doc)
           not_filtered_data = filtered_data
    return tuple([doc[0] for doc in filtered_data])


def get_query(new_param: dict = None) -> str:
    """Формирует запрос к БД, для изменения параметров объектов."""
    query_for_doc = '''
     UPDATE public.documents 
     SET processed_at = now() 
     WHERE documents.doc_id = %(document)s;
     '''

    if not new_param:
        return query_for_doc
    def get_parametrs(new_param: dict) -> str:
        values = []
        for field, value in new_param.items():
            str = f'{field} = %({field})s'
            values.append(str)
        return ', '.join(values)

    parametrs = get_parametrs(new_param)
    query_for_data = f'''
    UPDATE public.data 
    SET {parametrs} 
    WHERE data.object IN %(objects)s;'''
    return ' '.join((query_for_data, query_for_doc))


def get_values(new_data: dict, change_objects: tuple, doc_id: str) -> dict:
    """ Возвращает словарь со значениями для sql запроса."""
    new_data['objects'] = change_objects
    new_data['document'] = doc_id
    return new_data


def main():
    """Основная логика скрипта."""
    try:
        document = get_document()
        doc_id, objects, new_values, old_values = parce_document(document)
        data = get_data(objects)
        objects_for_change = check_data(data, old_values)
        if not objects_for_change:
            query = get_query()
        else:
            query = get_query(new_values)
        values_for_query = get_values(new_values, objects_for_change, doc_id)
        update_data(query, values_for_query)
        return True
    except:
        return False


if __name__ == '__main__':
    main()
