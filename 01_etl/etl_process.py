import os
import time
from config import settings, sql_query
from dotenv import load_dotenv
from etl_classes import (DataTransform, ElasticsearchLoader,
                         ElasticsearchPreparation, PostgresExtractor)

load_dotenv()

BATCH_SIZE = 25
INDEX_NAME = 'movies'


def etl():
    cl = ElasticsearchPreparation()
    index_name = INDEX_NAME
    postgr = PostgresExtractor(sql_query, BATCH_SIZE)

    el = ElasticsearchLoader(os.environ.get('ES_URL'), index_name)
    cl.create_index(index_name=index_name, settings=settings)
    transf = DataTransform(index_name)
    with postgr.conn as pc:
        rows = postgr.extract_data()
        for row in rows:
            res = transf.get_elasticsearch_type(row)
            el.upload_to_elasticsearch(res)
    pc.close()


if __name__ == '__main__':
    while True:
        etl()
        time.sleep(10)

