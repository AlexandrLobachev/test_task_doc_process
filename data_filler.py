import datetime
import json
import uuid
import random
import psycopg2


inns = ['owner_1', 'owner_2', 'owner_3', 'owner_4']
status = [1, 2, 3, 4, 10, 13]
d_type = ['transfer_document', 'not_transfer_document']


def make_data() -> dict:
    """Генерация рандомных данных для таблицы data в базе, вернёт list, внутри dict по каждой записи"""
    parents = set()
    children = dict()
    data_table = dict()

    for _ in list(range(0, 20)):
        parents.add('p_' + str(uuid.uuid4()))

    for p in parents:
        children[p] = set()
        for _ in list(range(0, 50)):
            children[p].add('ch_' + str(uuid.uuid4()))

    for k, ch in children.items():
        data_table[k] = {'object': k,
                         'status': random.choice(status),
                         'owner': random.choice(inns),
                         'level': 1,
                         'parent': None}

        for x in ch:
            data_table[x] = {'object': x,
                             'status': random.choice(status),
                             'owner': data_table[k]['owner'],
                             'level': 0,
                             'parent': k}
    return data_table


def make_documents(data: dict) -> list:
    """Генерация рандомных данных для таблицы documents в базе, вернёт list, внутри dict по каждой записи"""
    result = list()
    doc_count = random.choice(list(range(10, 20)))
    for _ in range(doc_count):
        result.append(__make_doc(data))
    return result


def __make_doc(data: dict) -> dict:
    saler = reciver = random.choice(inns)
    while saler == reciver:
        reciver = random.choice(inns)

    doc = dict()
    dd = doc['document_data'] = dict()
    dd['document_id'] = id = str(uuid.uuid4())
    dd['document_type'] = random.choice(d_type)

    doc['objects'] = [x for x, v in data.items() if v['level'] == 1 and v['owner'] == saler]

    md = doc['operation_details'] = dict()

    if random.choice([0, 1]):
        mds = md['status'] = dict()
        mds['new'] = mds['old'] = random.choice(status)
        while mds['old'] == mds['new']:
            mds['new'] = random.choice(status)

    if dd['document_type'] == d_type[0]:
        mdo = md['owner'] = dict()
        mdo['new'] = mdo['old'] = random.choice(inns)
        while mdo['old'] == mdo['new']:
            mdo['new'] = random.choice(inns)

    doc_data = {'doc_id': id,
                'recieved_at': datetime.datetime.now(),
                'document_type': dd['document_type'],
                'document_data': json.dumps(doc)}
    return doc_data

def load_data(data_tbl, documents_tbl):
    try:
        connection = psycopg2.connect(
            dbname='test_task_doc_db',
            host='localhost',
            user='db_user',
            password='password111',
        )
        print("Подключение установлено")
        cursor = connection.cursor()

        cursor.executemany(
            """INSERT INTO public.documents(
            doc_id,
            recieved_at,
            document_type,
            document_data
            ) VALUES (
            %(doc_id)s,
            %(recieved_at)s,
            %(document_type)s,
            %(document_data)s
            )""", documents_tbl)

        cursor.executemany(
            """INSERT INTO public.data(
            object,
            status,
            level,
            parent,
            owner
            ) VALUES (
            %(object)s,
            %(status)s,
            %(level)s,
            %(parent)s,
            %(owner)s
            )""", data_tbl)
        connection.commit()
        print("Объекты успешно загружены!")
    except (Exception, psycopg2.Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("Соединение с PostgreSQL закрыто")


if __name__ == '__main__':
    data = make_data()
    # данные для базы:
    data_tbl = list(data.values())
    documents_tbl = make_documents(data)
    load_data(data_tbl, documents_tbl)
