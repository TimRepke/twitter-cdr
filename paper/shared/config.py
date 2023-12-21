import os

from pydantic import ConfigDict
from pydantic_settings import BaseSettings


class Config(BaseSettings):
    HOST: str = 'localhost'  # host of the db server
    PORT: int = 5432  # port of the db server
    USER: str = 'nacsos'  # username for the database
    PASSWORD: str = 'secr€t_passvvord'  # password for the database user
    DATABASE: str = 'nacsos_core'  # name of the database

    PROJECT_ID: str
    BA_TECH: str
    BA_SENT: str

    class Config:
        case_sensitive = True
        env_prefix = 'TGEO_'
        extra = 'allow'


conf_file = os.environ.get('TGEO_PAPER_CONFIG', 'conf.env')
settings = Config(_env_file=conf_file, _env_file_encoding='utf-8')

__all__ = ['settings']
