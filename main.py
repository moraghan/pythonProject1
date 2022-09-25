import requests
import json
from sqlalchemy import Column, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from helpers import get_db_connection, get_api_key, get_request_types

Base = declarative_base()

class TMDBRequest(Base):
    __tablename__ = 'tmdb_requests'

    request_id = Column(Integer(), primary_key=True, autoincrement=True)
    request_type = Column(String(20), nullable=False)
    request_key = Column(Integer(), nullable=False)
    request_text = Column(String(100), nullable=False)
    response_json = Column(JSONB)
    __table_args__ = (UniqueConstraint('request_key', 'request_type', name='request_key_type_UK'),)

request_type = 'person'

DB_URL = get_db_connection()
engine = create_engine(DB_URL)
Base.metadata.create_all(engine)

REQUEST_TYPE_INFO = get_request_types()
API_KEY = get_api_key()

request_url = REQUEST_TYPE_INFO[request_type].URL
current_key = 100


with Session(engine) as session:
    while current_key <= 100000:

        if session.query(TMDBRequest).filter(TMDBRequest.request_key == current_key,
                                             TMDBRequest.request_type == request_type).first() is None:
            enriched_url = request_url.replace('{api_key}', API_KEY).replace('{id}', str(current_key))

            _response_data = requests.get(enriched_url)

            if _response_data.status_code == 200:
                response_data = json.loads(_response_data.text)

                request_1 = TMDBRequest(request_type=request_type,
                                    request_text=enriched_url,
                                    request_key=current_key,
                                    response_json=response_data)

                session.add(request_1)
                session.commit()
            else:
                response_data = 'Error'

        current_key = current_key + 1
        print(current_key)
session.commit()
