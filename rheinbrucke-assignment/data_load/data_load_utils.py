import io
import os
import glob
import time
import uuid
import json
from decimal import Decimal
from string import Template
import datetime
import psycopg2
import psycopg2.extras
from itertools import zip_longest

import logging

DB_HOST="localhost"
DB_USER="postgres"
DB_PASS="pgadmin"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(funcName)s()] %(levelname)s  %(message)s")

def write_dml_stmts_file(dml_file , dml_records):
    with io.open(dml_file, 'w') as dml_file_io:
        dml_file_io.writelines("%s\n" % stmt for stmt in dml_records)
    #Finished writing
    logging.info("Created file [%s] with [%s] records",dml_file,len(dml_records))
#End Function

def group_elements(n, iterable, padvalue='select 1;'):
    return zip_longest(*[iter(iterable)]*n, fillvalue=padvalue)

def save_db_list(dml_query_list,page_size):
    conn = psycopg2.connect( host=DB_HOST,database="rheinbrucke",user=DB_USER,password=DB_PASS)
    cur = conn.cursor()
    chunks = group_elements(page_size,dml_query_list)
    for chunk in chunks:
       dml_query_chunk = ' '.join(chunk)
       logging.info("Running DB Queries [%s] in a shot",len(chunk))
       cur.execute(dml_query_chunk) 
    cur.close()
    conn.commit()
    conn.close()
#End Function