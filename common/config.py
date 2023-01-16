import os
from pydantic import BaseSettings


class Secrets(BaseSettings):
    HOST: str = 'localhost'  # host of the db server
    PORT: int = 5432  # port of the db server
    USER: str = 'nacsos'  # username for the database
    PASSWORD: str = 'secr€t_passvvord'  # password for the database user
    DATABASE: str = 'nacsos_core'  # name of the database

    TWITTER_BEARER: str = ''

    DATA_RAW_TWEETS: str = 'data/geo/tweets/'

    PROJECT_ID: str
    USER_ID: str

    class Config:
        case_sensitive = True
        env_prefix = 'TGEO_'


conf_file = os.environ.get('TGEO_CONFIG', 'conf.env')
settings = Secrets(_env_file=conf_file, _env_file_encoding='utf-8')

__all__ = ['settings']
