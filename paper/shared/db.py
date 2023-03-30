from typing import Sequence, Any, TypeVar, Callable
from nacsos_data.db import DatabaseEngine
from sqlalchemy import TextClause, RowMapping
from sqlalchemy.orm import Session
from .config import settings

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
