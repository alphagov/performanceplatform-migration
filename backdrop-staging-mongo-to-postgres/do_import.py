from datetime import date, datetime
import json
import os
from random import shuffle
import ssl
import sys

import bson
from pymongo import MongoClient
import psycopg2

def to_json(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()

    if isinstance(obj, bson.objectid.ObjectId):
        return str(obj)

    raise TypeError("Type %s cannot be serialised" % type(obj))

VCAP = json.loads(os.environ['VCAP_SERVICES'])
PSQL_URI = VCAP['postgres'][0]['credentials']['uri']

PSQL_CONN = psycopg2.connect(PSQL_URI)
with PSQL_CONN.cursor() as psql_cursor:
    psql_cursor.execute("""
    CREATE TABLE IF NOT EXISTS mongo (
      id         VARCHAR   PRIMARY KEY,
      collection VARCHAR   NOT NULL,
      timestamp  TIMESTAMP NOT NULL,
      updated_at TIMESTAMP NOT NULL,
      record     JSONB     NOT NULL
    )
  """)
    PSQL_CONN.commit()

    psql_cursor.execute("""
    CREATE INDEX IF NOT EXISTS mongo_collection ON mongo (collection)
  """)
    PSQL_CONN.commit()

    psql_cursor.execute("""
    CREATE INDEX IF NOT EXISTS mongo_timestamp ON mongo (timestamp)
  """)
    PSQL_CONN.commit()

    psql_cursor.execute("""
    CREATE INDEX IF NOT EXISTS mongo_updated_at ON mongo (updated_at)
  """)
    PSQL_CONN.commit()

    psql_cursor.execute("""
    CREATE INDEX IF NOT EXISTS mongo_collection_timestamp ON mongo (collection, timestamp)
  """)
    PSQL_CONN.commit()

    psql_cursor.execute("""
    CREATE INDEX IF NOT EXISTS mongo_collection_updated_at ON mongo (collection, updated_at)
  """)
    PSQL_CONN.commit()
PSQL_CONN.close()

MONGO_URI = VCAP['mongodb'][0]['credentials']['uri']

if len(sys.argv) != 2:
    exit(1)

COLLECTION = sys.argv[1]
CHUNK_SIZE = 10000
MONGO = MongoClient(MONGO_URI, ssl=True, ssl_cert_reqs=ssl.CERT_NONE)
DB = MONGO[MONGO_URI.split('/')[-1]]

num_items_in_collection = DB[COLLECTION].find().count()
chunks = range(0, num_items_in_collection, CHUNK_SIZE)

psql_conn = psycopg2.connect(PSQL_URI)
with psql_conn.cursor() as psql_cursor:
    for ci in range(1, len(chunks)):
        records = DB[COLLECTION].find().limit(CHUNK_SIZE).skip(chunks[ci])
        records_str = [
            psql_cursor.mogrify(
                u"(%s, %s, %s, %s, %s)",
                (
                    u'{}:{}'.format(COLLECTION, r['_id']),
                    COLLECTION,
                    r[u'_timestamp'],
                    r[u'_updated_at'],
                    json.dumps(r, default=to_json)
                )
            )
            for r in records
            if u'_id' in r
            if u'_updated_at' in r
            if u'_timestamp' in r
        ]

        query = """INSERT INTO mongo
                   (id, collection, timestamp, updated_at, record)
                   VALUES """ + ",".join(
                           map(lambda s: s.decode('utf8'), records_str)
                   ) + " ON CONFLICT DO NOTHING"
        psql_cursor.execute(query)
        psql_conn.commit()

        print u'Committed {} items from collection "{}" to postgres'.format(
            psql_cursor.rowcount,
            COLLECTION
        )
psql_conn.close()
