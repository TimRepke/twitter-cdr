import logging
from enum import Enum
from pathlib import Path
from datetime import datetime

import typer
from pydantic import BaseModel
from sqlalchemy import text
import numpy as np
from matplotlib import pyplot as plt
import matplotlib.dates as mdates

from nacsos_data.db import DatabaseEngine

from common.vector_index import VectorIndex
from common.config import settings
from common.db_cache import QueryCache
from common.events import events
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
        vector_file_basename = Path(settings.DATA_VECTORS) / f'vec_2d_tsne'
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
        return [TweetInfo.parse_obj(r) for r in result]

    # def plot_sentiments_temp_all(counts: dict[str, list[SentimentCounts]],
    #                              relative: bool) -> plt.Figure:
    #     n_cols = 2
    #     n_rows = int(np.ceil(len(queries) / n_cols))
    #
    #     logger.info(f'Creating aggregate figure with {n_rows} rows and {n_cols} columns.')
    #
    #     fig, axes = plt.subplots(n_rows, n_cols, figsize=(4 * n_cols, 3 * n_rows))
    #     fig.suptitle(f'Sentiments per technology\n'
    #                  f'aggregation interval: "{interval}", smoothed across {smoothing_windowsize} intervals\n')
    #
    #     for i, (technology, tech_counts) in enumerate(counts.items()):
    #         row = i // n_cols
    #         col = i % n_cols
    #         logger.debug(f' -> plotting {technology} to row {row}; column {col}')
    #         ax = axes[row][col]
    #         ax.set_title(technology)
    #         plot_sentiments_temp_ax(ax, tech_counts, relative)
    #         ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    #
    #     fig.autofmt_xdate()
    #     fig.tight_layout()
    #     return fig

    logger.info(f'Loading index from {vector_file_basename}')
    index = VectorIndex()
    index.load(vector_file_basename)
    logger.debug('Building reverse lookup...')
    id2idx = {item_id: idx for idx, item_id in index.dict_labels.items()}
    min_x = np.min(index.vectors, axis=1)[0]
    min_y = np.min(index.vectors, axis=1)[1]
    max_x = np.max(index.vectors, axis=1)[0]
    max_y = np.max(index.vectors, axis=1)[1]
    logger.debug(f'Space spans: x = ({min_x}, {max_x}); y = ({min_y}, {max_y})')



    logger.info('Fetching tweet info...')
    data = fetch_tweet_info()



    figure = plot_sentiments_temp_all(data_acc, relative=True)
    show_save(figure, target_dir / 'sentiments_temporal' / f'tempo_{resolution.value}_rel_tech_all')
    figure = plot_sentiments_temp_all(data_acc, relative=False)
    show_save(figure, target_dir / 'sentiments_temporal' / f'tempo_{resolution.value}_abs_tech_all')


if __name__ == "__main__":
    typer.run(main)
