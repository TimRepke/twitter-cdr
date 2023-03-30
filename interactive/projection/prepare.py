import json
import logging
from enum import Enum
from pathlib import Path
from datetime import datetime

import pyarrow as pa
import pyarrow.feather as feather
import pyarrow.parquet as pq
import tqdm
import typer
from pydantic import BaseModel
from sqlalchemy import text
import numpy as np

from nacsos_data.db import DatabaseEngine

from common.vector_index import VectorIndex
from common.config import settings
from common.db_cache import QueryCache
from common.queries import queries


class LogLevel(str, Enum):
    DEBUG = 'DEBUG'
    WARN = 'WARN'
    INFO = 'INFO'
    ERROR = 'ERROR'


class TweetInfo(BaseModel):
    created_at: datetime
    twitter_id: str
    item_id: str
    sentiment: int
    technologies: list[int]
    twitter_author_id: str
    like_count: int
    quote_count: int
    reply_count: int
    retweet_count: int


def main(target_dir: str | None = None,
         vector_file_basename: str | None = None,
         start_time: str = '2010-01-01 00:00',
         end_time: str = '2022-12-31 23:59',
         space_buckets: int = 500,
         project_id: str | None = None,
         bot_annotation_tech: str = 'fc73da56-9f51-4d2b-ad35-2a01dbe9b275',
         bot_annotation_senti: str = 'e63da0c9-9bb5-4026-ab5e-7d5845cdc111',
         skip_cache: bool = False,
         log_level: LogLevel = LogLevel.DEBUG):
    if target_dir is None:
        target_dir = Path(settings.DATA_INTERACTIVE) / 'landscape'
    else:
        target_dir = Path(target_dir)

    if vector_file_basename is None:
        vector_file_basename = Path(settings.DATA_VECTORS) / f'vec_2d_tsne_mean_10_all'
    else:
        vector_file_basename = Path(vector_file_basename)

    if project_id is None:
        project_id = settings.PROJECT_ID

    logging.basicConfig(format='%(asctime)s [%(levelname)s] %(name)s: %(message)s', level=log_level.value)
    logging.getLogger('matplotlib.font_manager').setLevel('WARN')
    logging.getLogger('PIL.PngImagePlugin').setLevel('WARN')
    logger = logging.getLogger('timeline-tsne')
    logger.setLevel(log_level.value)

    target_dir = target_dir.resolve()
    logger.info(f'Ensuring target dir exists: {target_dir}')
    target_dir.mkdir(exist_ok=True, parents=True)

    db_engine = DatabaseEngine(host=settings.HOST, port=settings.PORT,
                               user=settings.USER, password=settings.PASSWORD,
                               database=settings.DATABASE)
    logger.info(f'Connecting to database {db_engine._connection_str}')

    query_cache = QueryCache(
        cache_dir=target_dir / 'cache',
        db_engine=db_engine,
        skip_cache=skip_cache
    )

    def fetch_tweet_info() -> list[TweetInfo]:
        query = text("""
            SELECT info.*
            FROM (SELECT DISTINCT ON (ti.twitter_id) ti.item_id,
                                                     ti.twitter_id,
                                                     ti.twitter_author_id,
                                                     ti.like_count,
                                                     ti.quote_count,
                                                     ti.reply_count,
                                                     ti.retweet_count,
                                                     ti.created_at,
                                                     ba_sent.value_int                                               as sentiment,
                                                     array_agg(ba_tech.value_int) OVER ( PARTITION BY ti.twitter_id) as technologies
                  FROM twitter_item ti
                           LEFT OUTER JOIN bot_annotation ba_tech on (
                              ti.item_id = ba_tech.item_id
                          AND ba_tech.bot_annotation_metadata_id = :bot_tech
                          AND ba_tech.key = 'tech')
                           LEFT JOIN bot_annotation ba_sent on (
                              ba_tech.item_id = ba_sent.item_id
                          AND ba_sent.bot_annotation_metadata_id = :bot_senti
                          AND ba_sent.repeat = 1
                          AND ba_sent.key = 'senti')
                  WHERE ti.project_id = :project_id
                    AND ti.created_at >= :start_time ::timestamp
                    AND ti.created_at <= :end_time ::timestamp) info
            ORDER BY info.created_at;
        """)
        result = query_cache.query(query, {
            'start_time': start_time,
            'end_time': end_time,
            'project_id': project_id,
            'bot_senti': bot_annotation_senti,
            'bot_tech': bot_annotation_tech,
        })
        return [TweetInfo(item_id=str(r['item_id']), created_at=r['created_at'], sentiment=r['sentiment'],
                          technologies=r['technologies'], twitter_id=r['twitter_id'],
                          twitter_author_id=r['twitter_author_id'], like_count=r['like_count'],
                          quote_count=r['quote_count'], reply_count=r['reply_count'], retweet_count=r['retweet_count'])
                for r in result]

    logger.info(f'Loading index from {vector_file_basename}')
    index = VectorIndex()
    index.load(vector_file_basename)
    logger.debug('Building reverse lookup...')
    id2idx = {item_id: idx for idx, item_id in index.dict_labels.items()}
    x = index.vectors[:, 0]
    y = index.vectors[:, 1]
    del index

    min_x = np.min(x)
    min_y = np.min(y)
    max_x = np.max(x)
    max_y = np.max(y)
    range_x = abs((min_x - min_x) - (max_x - min_x))
    range_y = abs((min_y - min_y) - (max_y - min_y))

    logger.debug(f'Space spans: x = ({min_x}, {max_x} | {range_x}); y = ({min_y}, {max_y} | {range_y})')

    x_norm = (x - min_x) / range_x
    y_norm = (y - min_y) / range_y
    x_scaled = (x_norm * (space_buckets - 1)).round()
    y_scaled = (y_norm * (space_buckets - 1)).round()

    x_scaled = x_scaled.astype(int)
    y_scaled = y_scaled.astype(int)

    logger.info('Fetching tweet info...')
    data: list[TweetInfo] = fetch_tweet_info()
    logger.info('Joining vectors and tweet info...')
    matched: list[dict] = [
        {
            **d.dict(),
            'twitter_id': int(d.twitter_id),
            'twitter_author_id': int(d.twitter_author_id),
            # 'x': x[id2idx[d.item_id]],
            # 'y': y[id2idx[d.item_id]],
            'xb': x_scaled[id2idx[d.item_id]],
            'yb': y_scaled[id2idx[d.item_id]]
        }
        for d in tqdm.tqdm(data)
        if d.item_id in id2idx
    ]
    del data

    logger.debug('from_pylist()')
    tab = pa.Table.from_pylist(
        mapping=matched,
        schema=pa.schema(
            fields=[
                pa.field('created_at', pa.date64()),
                pa.field('item_id', pa.string()),
                pa.field('twitter_id', pa.uint64()),
                pa.field('twitter_author_id', pa.uint64()),
                pa.field('sentiment', pa.uint8()),
                pa.field('technologies', pa.list_(pa.uint8())),
                pa.field('like_count', pa.uint64()),
                pa.field('quote_count', pa.uint64()),
                pa.field('reply_count', pa.uint64()),
                pa.field('retweet_count', pa.uint64()),
                # pa.field('x', pa.float32()),
                # pa.field('y', pa.float32()),
                pa.field('xb', pa.uint16()),
                pa.field('yb', pa.uint16())
            ],
            metadata={
                'range_xb': json.dumps([0, 500]),
                'range_yb': json.dumps([0, 500]),
                'range_x': json.dumps([min_x, max_x]),
                'range_y': json.dumps([min_y, max_y]),
                'sentiments': json.dumps(['negative', 'neutral', 'positive']),
                'technologies': json.dumps(list(queries.keys())),
                'dt_start': start_time,
                'dt_end': end_time
            }
        )
    )

    logger.debug(tab.schema)

    logger.info('Writing feather...')
    target_dir.mkdir(parents=True, exist_ok=True)
    feather.write_feather(tab, str(target_dir / 'data.feather'), compression='lz4')
    logger.debug('Done!')

    logger.debug('Writing arrow...')
    writer = pa.RecordBatchFileWriter(str(target_dir / 'data.arrow'), tab.schema)
    writer.write(tab)
    writer.close()

    logger.debug('Writing parquet...')
    pq.write_table(tab, str(target_dir / 'data.parquet'))
    logger.debug('Done!')

if __name__ == "__main__":
    typer.run(main)
