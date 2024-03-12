from main import *

DOCUMENT = ('id_doc_1',
     (2024, 3, 11, 11, 37, 37, 867094),
     'transfer_document',
     {'objects': [
         'id_parent_1',
         'id_parent_2',
         'id_parent_3',
         'id_parent_4',
         'id_parent_5'],
         'document_data': {
             'document_id': 'id_doc_1',
             'document_type': 'transfer_document'
         },
         'operation_details': {
             'owner': {'new': 'owner_2', 'old': 'owner_1'},
             'status': {'new': 2, 'old': 1}
         }},
     None)

DATA = [('id_ch_1', 1, 0, 'id_parent_1', 'owner_4'),
        ('id_ch_2', 4, 0, 'id_parent_1', 'owner_99'),
        ('id_ch_3', 2, 0, 'id_parent_1', 'owner_4'),
        ('id_ch_4', 4, 0, 'id_parent_1', 'owner_99'),
        ('id_ch_5', 1, 0, 'id_parent_1', 'owner_4'),
        ('id_ch_6', 99, 0, 'id_parent_1', 'owner_99'),
        ('id_ch_7', 99, 0, 'id_parent_1', 'owner_4'),
        ('id_ch_8', 99, 0, 'id_parent_1', 'owner_99'),
        ('id_ch_9', 99, 0, 'id_parent_1', 'owner_4')]

OLD_VALUES = {'owner': 'owner_99', 'status': 99}

NEW_VALUES = {'owner': 'owner_99', 'status': 99}

OBJECTS_FOR_CHANGE = ('id_ch_6', 'id_ch_8')

DOC_ID = 'id_doc_1'



def test_parce_document():
    assert parce_document(DOCUMENT) == ('id_doc_1',
                                        ('id_parent_1',
                                         'id_parent_2',
                                         'id_parent_3',
                                         'id_parent_4',
                                         'id_parent_5'),
                                        {'owner': 'owner_2',
                                         'status': 2},
                                        {'owner': 'owner_1',
                                         'status': 1}
                                        )


def test_check_dataa():
    assert check_data(DATA, OLD_VALUES) == ('id_ch_6', 'id_ch_8')


def test_get_query_wo_val():
    assert get_query() == '''
    UPDATE public.documents SET processed_at = now() WHERE documents.doc_id = %(document)s;
    '''


def test_get_query_with_val():
    assert get_query(NEW_VALUES) == ' '.join(
        ('''UPDATE public.data SET owner = %(owner)s, status = %(status)s WHERE data.object IN %(objects)s;''',
        '''
    UPDATE public.documents SET processed_at = now() WHERE documents.doc_id = %(document)s;
    '''))


def test_get_values():
    assert get_values(NEW_VALUES, OBJECTS_FOR_CHANGE, DOC_ID) == {
        'owner': 'owner_99',
        'status': 99,
        'objects': ('id_ch_6', 'id_ch_8'),
        'document': 'id_doc_1'
    }


def test_get_doc_and_data(load_data):
    document = get_document()
    assert document[0] == 'good_doc'
    _, objects, _, _ = parce_document(document)
    data = get_data(objects)
    assert data[0][0] == 'good_obj'


def test_main():
    assert main() == True


def test_check_changed_data(get_changed_data):
    assert get_changed_data == ('good_obj', 99, 0, 'id_parent_1', 'owner_99')


def test_chek_changed_doc(get_changed_doc):
    assert get_changed_doc == ('good_doc',)
