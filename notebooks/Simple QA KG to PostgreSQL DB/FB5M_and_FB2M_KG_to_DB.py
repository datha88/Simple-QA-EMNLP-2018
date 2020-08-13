
# coding: utf-8

# # FB5M / FB2M KG to DB
# 
# 

# In[3]:

import sys
sys.path.insert(0, '../../')
from lib.utils import FB5M_KG
from lib.utils import FB2M_KG
from lib.utils import FB2M_KG_TABLE
from lib.utils import FB5M_KG_TABLE
from lib.utils import get_connection 

connection = get_connection()
cursor = connection.cursor()

tables = [(FB5M_KG_TABLE, FB5M_KG), (FB2M_KG_TABLE, FB2M_KG)]


# In[4]:

for (table_name, _) in tables:
    cursor.execute("""
        CREATE TABLE %s
            (object_mid varchar NOT NULL,
            relation varchar NOT NULL,
            subject_mid varchar NOT NULL,
            PRIMARY KEY(object_mid, relation, subject_mid));""" % (table_name,))


# In[ ]:

from tqdm import tqdm_notebook

chunk_size = 20000

def insert_chunk(rows, table_name):
    insert_query = 'INSERT INTO ' + table_name + ' (object_mid, relation, subject_mid) VALUES %s ON CONFLICT DO NOTHING;'
    psycopg2.extras.execute_values(
        cursor, insert_query, rows, template=None, page_size=100
    )
    
for (table_name, data_path) in tables:
    rows = []
    for line in tqdm_notebook(open(data_path, 'r'), total=12010500):
        # Build Chunks
        split = line.split('\t')
        assert len(split) == 3, 'Malformed row'
        subject = split[0].replace('www.freebase.com/m/', '').strip()
        property_ = split[1].replace('www.freebase.com/', '').strip()
        objects = [url.replace('www.freebase.com/m/', '').strip() for url in split[2].split()]
        rows.extend([tuple([str(object_), str(property_), str(subject)]) for object_ in objects])

        # Insert Chunk
        if len(rows) > chunk_size:
            insert_chunk(rows, table_name)
            rows = []

    insert_chunk(rows, table_name)


# In[ ]:

connection.commit()


# Add indexes to the DB that will be useful in other notebooks.

# In[ ]:

for (table_name, _) in tables:
    cursor.execute("""
        CREATE INDEX %s_relation_index ON %s (relation);
        CREATE INDEX %s_subject_mid_index ON %s (subject_mid);""" % (table_name, table_name, 
                                                                     table_name, table_name))
    connection.commit()


# In[ ]:

cursor.close()
connection.close()

