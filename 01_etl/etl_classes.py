import uuid
import datetime
from dataclasses import dataclass
import psycopg2
import psycopg2.extras
import requests
import logging
from elasticsearch import Elasticsearch
from state import JsonFileStorage, State
from backoff_ import backoff
import os
import json
from psycopg2 import sql
from dotenv import load_dotenv
from typing import Dict
load_dotenv()

logging.basicConfig(filename="elastic.log", level=logging.DEBUG, format='%(asctime)s %(levelname)s:%(message)s')


class ElasticsearchPreparation:
    def __init__(self):
        self.client = Elasticsearch(
            "http://{0}:{1}".format(os.environ.get('ES_HOST'), os.environ.get('ES_PORT')),
        )
        logging.info(self.client.ping())

    @backoff()
    def create_index(self, index_name: str, settings: Dict) -> bool:
        """Создать индекс для Elasticsearch"""
        created = False
        try:
            if not self.client.indices.exists(index_name):
                logging.info("Creating index {0} with schema {1}".format(index_name, settings))
                self.client.indices.create(index=index_name, ignore=400, body=settings)
            created = True
        except Exception as ex:
            logging.error(str(ex))
        finally:
            return created


@backoff()
def postgres_connection(db_sttngs):
    return psycopg2.connect(**db_sttngs)


class PostgresExtractor:
    def __init__(self, db_sttngs: dict, query: str, batch_size: int):
        self.query = query
        self.db_sttngs = db_sttngs
        self.batch_size = batch_size
        self.conn = postgres_connection(self.db_sttngs)

    def get_state(self):
        storage = JsonFileStorage("state.json")
        state = State(storage)
        return {'state': state.get_state('modified')}

    def extract_data(self):
        """Генератор пачек данных"""
        conn = postgres_connection(self.db_sttngs)
        with conn.cursor() as curs:
            curs.execute(sql.SQL(self.query), self.get_state())

            while True:
                rows = curs.fetchmany(self.batch_size)
                if not rows:
                    break
                yield rows


@dataclass
class Movies:
    id: uuid.UUID
    title: str
    description: str
    rating: int
    type: str
    created: datetime.datetime
    modified: datetime.datetime
    persons: list
    genres: list


class DataTransform:
    def __init__(self, index_name: str):
        self.index_name = index_name

    def get_elasticsearch_type(self, rows):
        """Метод для возврата типа подходящего для ElasticSearch"""
        result = []
        for row in rows:
            movie_info = Movies(*row)
            director = [d['person_name'] for d in movie_info.persons if d['person_role'] == 'director']
            actors_names = [a['person_name'] for a in movie_info.persons if a['person_role'] == 'actor']
            writers_names = [w['person_name'] for w in movie_info.persons if w['person_role'] == 'writer']
            actors = [{"id": a['person_id'], "name": a['person_name']}
                      for a in movie_info.persons if a['person_role'] == 'actor']
            writers = [{"id": w['person_id'], "name": w['person_name']}
                       for w in movie_info.persons if w['person_role'] == 'writer']
            res = {
                'id': movie_info.id,
                'imdb_rating': movie_info.rating,
                'genre': movie_info.genres,
                'title': movie_info.title,
                'description': movie_info.description,
                'director': director,
                'actors_names': actors_names,
                'writers_names': writers_names,
                'actors': actors,
                'writers': writers}
            result.append(res)
        return result


def bulk(rows: list, index_name: str):
    """Создание запроса для закачивания данных в ElasticSearch"""
    query = []
    for row in rows:
        query.extend([json.dumps({'index': {'_index': index_name, '_id': row['id']}}), json.dumps(row)])
    return query


class ElasticsearchLoader:
    def __init__(self, url: str, index_name: str):
        self.url = url
        self.index_name = index_name

    def set_state(self):
        storage = JsonFileStorage("state.json")
        state = State(storage)
        state.set_state("modified", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    @backoff()
    def upload_to_elasticsearch(self, rows: list):
        """Закачивание данных в ElasticSearch через посыл запроса"""
        query = bulk(rows, self.index_name)
        response = requests.post(
            self.url + '_bulk',
            data='\n'.join(query) + '\n',
            headers={'Content-Type': 'application/x-ndjson'}
        )
        logging.info(response.text)
        if response.status_code == 200:
            self.set_state()




