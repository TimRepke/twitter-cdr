import logging
from enum import Enum
from pathlib import Path
from datetime import datetime

import tqdm
import typer
from pydantic import BaseModel
from sqlalchemy import text
import numpy as np
from matplotlib import pyplot as plt

from nacsos_data.db import DatabaseEngine

from common.vector_index import VectorIndex
from common.config import settings
from common.db_cache import QueryCache


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


def main(target_dir: str | None = None,
         vector_file_basename: str | None = None,
         start_time: str = '2010-01-01 00:00',
         end_time: str = '2022-12-31 23:59',
         bucket_fmt: str = '%Y-%W',  # https://docs.python.org/3/library/datetime.html#strftime-strptime-behavior
         space_buckets: int = 500,
         show: bool = False,
         export_png: bool = True,
         export_pdf: bool = False,
         export_svg: bool = False,
         project_id: str | None = None,
         bot_annotation_tech: str = 'fc73da56-9f51-4d2b-ad35-2a01dbe9b275',
         bot_annotation_senti: str = 'e63da0c9-9bb5-4026-ab5e-7d5845cdc111',
         skip_cache: bool = False,
         log_level: LogLevel = LogLevel.DEBUG):
    if target_dir is None:
        target_dir = Path(settings.DATA_FIGURES) / 'landscape'
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

    def show_save(fig: plt.Figure, target: Path):
        target.parent.mkdir(parents=True, exist_ok=True)

        if export_png:
            fig.savefig(str(target) + '.png')
        if export_svg:
            fig.savefig(str(target) + '.svg')
        if export_pdf:
            fig.savefig(str(target) + '.pdf')
        if show:
            fig.show()
        else:
            plt.close(fig)

    def fetch_tweet_info() -> list[TweetInfo]:
        query = text("""
            SELECT info.*
            FROM (SELECT DISTINCT ON (ti.twitter_id) ti.item_id,
                                                     ti.twitter_id,
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
                          technologies=r['technologies'], twitter_id=r['twitter_id']) for r in result]

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
    data = fetch_tweet_info()

    logger.info('Joining vectors and tweet info...')
    matched: list[tuple[int, TweetInfo]] = [
        (id2idx[d.item_id], d)
        for d in tqdm.tqdm(data)
        if d.item_id in id2idx
    ]
    del data

    logger.debug('Finding time buckets...')
    buckets = sorted(list(set([d.created_at.strftime(bucket_fmt) for _, d in matched])))
    logger.debug(f' -> Found {len(buckets)} time buckets')

    logger.info('Building timeline matrix (Y)')
    y_timeline = np.zeros((space_buckets, len(buckets)))
    for di, d in tqdm.tqdm(matched):
        y_timeline[y_scaled[di]][buckets.index(d.created_at.strftime(bucket_fmt))] += 1

    logger.debug(f'sum: {y_timeline.sum():,.0f}, mean: {y_timeline.mean()}')
    figure, ax = plt.subplots(figsize=(10, 10))
    ax.imshow(y_timeline, cmap='YlOrRd', vmax=10)
    show_save(figure, target_dir / 'temporal' / f'tempo_y')

    logger.info('Building timeline matrix (X)')
    x_timeline = np.zeros((space_buckets, len(buckets)))
    for di, d in tqdm.tqdm(matched):
        x_timeline[x_scaled[di]][buckets.index(d.created_at.strftime(bucket_fmt))] += 1

    logger.debug(f'sum: {x_timeline.sum():,.0f}, mean: {x_timeline.mean()}')
    figure, ax = plt.subplots(figsize=(10, 10))
    ax.imshow(x_timeline, cmap='YlOrRd', vmax=10)
    show_save(figure, target_dir / 'temporal' / f'tempo_x')

    logger.info('Building rasterised scatterplot matrix...')
    scatter_im = np.zeros((space_buckets, space_buckets))
    for di, _ in tqdm.tqdm(matched):
        scatter_im[y_scaled[di]][x_scaled[di]] += 1

    print(scatter_im.shape)
    print(x_timeline.T.shape)
    print(y_timeline.shape)

    figure, axes = plt.subplots(2, 2, sharex='col', sharey='row', figsize=(12, 12),
                                gridspec_kw={'width_ratios': (1, 1.4), 'height_ratios': (1, 1.4)}, )
    ax_scatter = axes[0][0]
    ax_tlx = axes[1][0]
    ax_tly = axes[0][1]
    axes[1][1].remove()
    ax_scatter.imshow(scatter_im, cmap='YlOrRd', vmax=20)
    ax_tlx.imshow(x_timeline.T, cmap='YlOrRd', vmax=10)
    ax_tly.imshow(y_timeline, cmap='YlOrRd', vmax=10)
    ax_scatter.tick_params(axis='x', labelbottom=False, labeltop=True)
    ax_scatter.tick_params(axis='y', labelright=False, labelleft=True)
    ax_tlx.tick_params(axis='x', labelbottom=False, labeltop=False)
    ax_tlx.tick_params(axis='y', labelright=True, labelleft=False)
    ax_tly.tick_params(axis='x', labelbottom=True, labeltop=False)
    ax_tly.tick_params(axis='y', labelright=False, labelleft=False)
    figure.tight_layout()

    show_save(figure, target_dir / 'temporal' / f'tempo_both')

if __name__ == "__main__":
    typer.run(main)
