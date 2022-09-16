from etl_classes import DataTransform
from etl_classes import PostgresExtractor
from config import database, sql_query
from etl_classes import ElasticsearchLoader
from etl_classes import ElasticsearchPreparation
from config import settings
from dotenv import load_dotenv
import os

load_dotenv()


if __name__ == '__main__':
    cl = ElasticsearchPreparation()
    index_name = 'movies'
    postgr = PostgresExtractor(database, sql_query, 25)

    el = ElasticsearchLoader(os.environ.get('ES_URL'), index_name)
    cl.create_index(index_name=index_name, settings=settings)
    transf = DataTransform(index_name)
    with postgr.conn as pc:
        rows = postgr.extract_data()
        for row in rows:
            res = transf.get_elasticsearch_type(row)
            el.upload_to_elasticsearch(res)
    pc.close()
