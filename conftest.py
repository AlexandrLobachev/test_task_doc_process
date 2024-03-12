import psycopg2
import datetime
import pytest


CREDS = {
    'dbname': 'test_task_doc_db',
    'host': 'localhost',
    'user': 'db_user',
    'password': 'password111',
}

DOCUMENTS = [{
    'doc_id': 'good_doc',
    'recieved_at': datetime.datetime.now() - datetime.timedelta(weeks=100),
    'document_type': 'transfer_document',
    'document_data': '{'
                '"document_data": {'
                     '"document_id": "good_doc", '
                     '"document_type": "transfer_document"'
                     '}, '
                '"objects": ["id_parent_1", "id_parent_2"], '
                '"operation_details": {'
                     '"status": {"new": 99, "old": 1}, '
                     '"owner": {"new": "owner_99", "old": "owner_1"}}}'},
    {'doc_id': 'wrong_doc_1',
     'recieved_at': datetime.datetime.now() - datetime.timedelta(weeks=99),
     'document_type': 'transfer_document',
     'document_data': '{"document_data": {"document_id": "wrong_doc_1"}}'},
    {'doc_id': 'wrong_doc_2',
     'recieved_at': datetime.datetime.now() - datetime.timedelta(weeks=98),
     'document_type': 'no_transfer_document',
     'document_data': '{"document_data": {"document_id": "wrong_doc_2"}}'}]

DATA = [
    {'object': 'good_obj', 'status': 1, 'owner': 'owner_1', 'level': 0, 'parent': 'id_parent_1'},
    {'object': 'wrong_0001', 'status': 2, 'owner': 'owner_1', 'level': 0, 'parent': 'id_parent_1'},
    {'object': 'wrong_0002', 'status': 1, 'owner': 'owner_2', 'level': 0, 'parent': 'id_parent_1'},
    {'object': 'wrong_0003', 'status': 2, 'owner': 'owner_1', 'level': 0, 'parent': 'id_parent_1000'},
    {'object': 'wrong_0004', 'status': 4, 'owner': 'owner_4', 'level': 1, 'parent': None}
]


@pytest.fixture
def load_data():
    try:
        conn = psycopg2.connect(**CREDS)
        with conn.cursor() as curs:

            curs.executemany(
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
                )""", DOCUMENTS)

            curs.executemany(
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
                )""", DATA)
        conn.commit()
    finally:
        curs.close()
        conn.close()


def clear_db():
    try:
        conn = psycopg2.connect(**CREDS)
        with conn.cursor() as curs:
            curs.execute("""
            DELETE FROM documents
            WHERE doc_id IN ('good_doc', 'wrong_doc_1', 'wrong_doc_2')
            """)
            curs.execute("""
            DELETE FROM data
            WHERE object IN ('good_obj', 'wrong_0001', 'wrong_0002','wrong_0003','wrong_0004')""")
            conn.commit()

    finally:
        curs.close()
        conn.close()


@pytest.fixture
def get_changed_data():
    try:
        conn = psycopg2.connect(**CREDS)
        with conn.cursor() as curs:
            curs.execute(
                '''
                SELECT * FROM public.data
                WHERE status = 99 OR owner = 'owner_99'
                ''')
            return curs.fetchone()
    finally:
        curs.close()
        conn.close()


@pytest.fixture
def get_changed_doc():
    try:
        conn = psycopg2.connect(**CREDS)
        with conn.cursor() as curs:
            curs.execute(
                '''
                SELECT doc_id FROM public.documents
                WHERE processed_at NOTNULL
                ORDER BY processed_at DESC;
                ''')

            yield curs.fetchone()
            clear_db()
    finally:
        curs.close()
        conn.close()
