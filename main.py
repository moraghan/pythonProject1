import requests
import argparse
import json
from sqlalchemy import Column, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, func
from sqlalchemy.orm import Session

from helpers import get_db_connection, get_api_key, get_request_types

Base = declarative_base()

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:20.0) Gecko/20100101 Firefox/20.0",
    "Accept-Encoding": "gzip, deflate",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Connection": "keep-alive"
}


class TMDBRequest(Base):
    __tablename__ = 'tmdb_requests'

    request_id = Column(Integer(), primary_key=True, autoincrement=True)
    request_type = Column(String(20), nullable=False)
    request_key = Column(Integer(), nullable=False)
    request_text = Column(String(100), nullable=False)
    response_json = Column(JSONB)
    __table_args__ = (UniqueConstraint('request_key', 'request_type', name='request_key_type_UK'),)


def main(request_type):
    DB_URL = get_db_connection()
    engine = create_engine(DB_URL)
    Base.metadata.create_all(engine)

    request_type_info = get_request_types()
    api_key = get_api_key()

    request_url = request_type_info[request_type].URL
    max_request_key = request_type_info[request_type].MAX_REQUEST_KEY

    with Session(engine) as session:

        _last_key_request = session.query(
            func.max(TMDBRequest.request_key).filter(TMDBRequest.request_type == request_type)).one()[0]

        if _last_key_request is None:
            current_key = 1
        else:
            current_key = int(_last_key_request) + 1

        while current_key <= max_request_key:

            if session.query(TMDBRequest).filter(TMDBRequest.request_key == current_key,
                                                 TMDBRequest.request_type == request_type).first() is None:
                enriched_url = request_url.replace('{api_key}', api_key).replace('{id}', str(current_key))
                print(enriched_url)
                print(f'Retrieving data for request type {request_type} and key {current_key}')

                try:
                    _response_data = requests.get(enriched_url, headers=headers)

                    if _response_data.status_code == 200:
                        response_data = json.loads(_response_data.text)

                        TMDB_request_to_add = TMDBRequest(request_type=request_type,
                                                          request_text=enriched_url,
                                                          request_key=current_key,
                                                          response_json=response_data)

                        session.add(TMDB_request_to_add)
                        session.commit()
                except requests.exceptions.RequestException as e:
                    print(f'**ERROR Retrieving data for request type {request_type} with key {current_key}')
            else:
                print(f'Data has already been retrieved for type {request_type} with key {current_key}')

            current_key = current_key + 1

    session.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Extract movie and person details from TMDB', prog='Main',
                                     usage='%(prog)s [options] request_type')
    parser.add_argument('request_type', type=str, help='Request Type: Either movie or person',
                        choices=['movie', 'person', 'company', 'credit'], default='person')
    args = parser.parse_args()
    print(args)
    main(args.request_type)
