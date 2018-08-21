from datetime import date, datetime
import json
import os
from random import shuffle
import ssl

import bson
from pymongo import MongoClient
import psycopg2

def to_json(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()

    if isinstance(obj, bson.objectid.ObjectId):
        return str(obj)

    raise TypeError("Type %s cannot be serialised" % type(obj))

WORKER_COUNT = int(os.environ['WORKER_COUNT'])
WORKER_INDEX = int(os.environ['CF_INSTANCE_INDEX'])
VCAP = json.loads(os.environ['VCAP_SERVICES'])
PSQL_URI = VCAP['postgres'][0]['credentials']['uri']

PSQL_CONN = psycopg2.connect(PSQL_URI)
with PSQL_CONN.cursor() as psql_cursor:
    psql_cursor.execute("""
    CREATE TABLE IF NOT EXISTS mongo (
      id         VARCHAR PRIMARY KEY,
      collection VARCHAR NOT NULL,
      record     JSON NOT NULL
    )
  """)
    PSQL_CONN.commit()
PSQL_CONN.close()

MONGO_URI = VCAP['mongodb'][0]['credentials']['uri']
COLLECTION = 'govuk_pay_payments'

MONGO = MongoClient(MONGO_URI, ssl=True, ssl_cert_reqs=ssl.CERT_NONE)
DB = MONGO[MONGO_URI.split('/')[-1]]

CHUNK_SIZE = 10000

COLLECTIONS = sorted(DB.collection_names())
COLLECTIONS_PER_WORKER = len(COLLECTIONS) // WORKER_COUNT

WORKER_COLL_START = COLLECTIONS_PER_WORKER * WORKER_INDEX
WORKER_COLL_END = COLLECTIONS_PER_WORKER * (WORKER_INDEX + 1)

WORKER_COLLECTIONS = shuffle(COLLECTIONS[WORKER_COLL_START:WORKER_COLL_END])

for index, cname in enumerate(WORKER_COLLECTIONS):
    num_items_in_collection = DB[cname].find().count()
    chunks = range(0, num_items_in_collection, CHUNK_SIZE)

    psql_conn = psycopg2.connect(PSQL_URI)
    with psql_conn.cursor() as psql_cursor:
        for ci in range(1, len(chunks)):
            records = DB[cname].find().limit(CHUNK_SIZE).skip(chunks[ci])
            records_str = [
                psql_cursor.mogrify(
                    "(%s, %s, %s)",
                    (
                        '{}:{}'.format(cname, r['_id']),
                        cname,
                        json.dumps(r, default=to_json)
                    )
                )
                for r in records
                if '_id' in r
            ]

            query = """INSERT INTO mongo
                       (id, collection, record)
                       VALUES """ + ",".join(records_str) + " ON CONFLICT DO NOTHING"
            psql_cursor.execute(query)
            psql_conn.commit()

            print 'Committed {} items from collection "{}" to postgres'.format(
                psql_cursor.rowcount,
                cname
            )
    psql_conn.close()
