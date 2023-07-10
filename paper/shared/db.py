import pickle
import logging
from pathlib import Path
from typing import Sequence, Any, TypeVar, Callable
from nacsos_data.db import DatabaseEngine
from sqlalchemy import TextClause, RowMapping
from sqlalchemy.orm import Session
from .config import settings

logger = logging.getLogger('db')
RowType = TypeVar('RowType')

db_engine = DatabaseEngine(host=settings.HOST, port=settings.PORT,
                           user=settings.USER, password=settings.PASSWORD,
                           database=settings.DATABASE)


def run_query(query: TextClause,
              params: dict[str, Any],
              row2obj: Callable[[dict[str, Any]], RowType] | None = None) \
        -> Sequence[RowMapping] | list[dict] | list[RowType]:
    with db_engine.session() as session:  # type: Session
        if row2obj:
            return [row2obj(dict(r)) for r in session.execute(query, params).mappings().all()]
        else:
            return [dict(r) for r in session.execute(query, params).mappings().all()]


def get_data(query: TextClause,
             file: Path | str,
             params: dict[str, Any]):
    file = Path(file)

    if file.exists():
        logger.debug(f'Cache file exists, loading data from there: {file}')
        with open(file, 'rb') as fin:
            return pickle.load(fin)

    logger.debug(f'Cache file does not exist, loading data from db')
    result = run_query(query, params)

    logger.debug(f'Caching data at: {file}')
    file.parent.mkdir(parents=True, exist_ok=True)
    with open(file, 'wb') as fout:
        pickle.dump(result, fout)

    return result
