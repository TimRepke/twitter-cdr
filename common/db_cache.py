import json
import logging
import hashlib
import inspect
from pathlib import Path
from typing import Sequence, Any, TextIO
import pickle

from nacsos_data.db import DatabaseEngine
from sqlalchemy import TextClause, RowMapping
from sqlalchemy.orm import Session

logger = logging.getLogger('db-cache')


def make_hash_sha256(o):
    hasher = hashlib.sha256()
    hasher.update(repr(make_hashable(o)).encode())
    # return base64.b64encode(hasher.digest()).decode()
    return hasher.hexdigest()


def make_hashable(o):
    if isinstance(o, (tuple, list)):
        return tuple((make_hashable(e) for e in o))
    if isinstance(o, dict):
        return tuple(sorted((k, make_hashable(v)) for k, v in o.items()))
    if isinstance(o, (set, frozenset)):
        return tuple(sorted(make_hashable(e) for e in o))
    return o


def run_query(query: TextClause,
              params: dict[str, Any],
              db_engine: DatabaseEngine,
              cache_dir: Path,
              skip_cache: bool,
              caller_offset: int = 1) -> Sequence[RowMapping] | list[dict]:
    caller = inspect.stack()[caller_offset].function
    param_hash = make_hash_sha256(params)

    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_file = cache_dir / f'{caller}__{param_hash}.pkl'

    if skip_cache:
        logger.debug('Deleting cache file in skip_cache mode...')
        cache_file.unlink(missing_ok=True)

    if cache_file.is_file():
        logger.debug(f'Reading query result from cache cache for caller {caller} (file: {cache_file})')

        with open(cache_file, 'rb') as f:
            return pickle.load(f)
            # return [json.loads(line) for line in f]

    with db_engine.session() as session:  # type: Session
        logger.debug(f'Fetching data from db for caller {caller} (will store cache at: {cache_file})')
        results = [dict(r) for r in session.execute(query, params).mappings().all()]

    with open(cache_file, 'wb') as f_out:
        pickle.dump(results, f_out)
        # for result in results:
        #     f_out.write(json.dumps(result) + '\n')
        return results


class QueryCache:
    def __init__(self,
                 db_engine: DatabaseEngine,
                 cache_dir: Path,
                 skip_cache: bool):
        self.db_engine = db_engine
        self.cache_dir = cache_dir
        self.skip_cache = skip_cache

    def query(self, query: TextClause,
              params: dict[str, Any]) -> Sequence[RowMapping] | list[dict]:
        return run_query(query, params,
                         db_engine=self.db_engine,
                         cache_dir=self.cache_dir,
                         skip_cache=self.skip_cache,
                         caller_offset=2)
