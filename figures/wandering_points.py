import logging
from enum import Enum
from pathlib import Path
from typing import TypedDict
import datetime

import typer
from pydantic import BaseModel
from sqlalchemy import text
import numpy as np
from matplotlib import pyplot as plt
import matplotlib.axes as mpla
import matplotlib.dates as mdates
import plotly.graph_objects as go
from matplotlib.animation import FuncAnimation, PillowWriter
from scipy.interpolate import make_interp_spline

from nacsos_data.db import DatabaseEngine

from common.config import settings
from common.db_cache import QueryCache
from common.queries import queries
from common.vector_index import VectorIndex


class LogLevel(str, Enum):
    DEBUG = 'DEBUG'
    WARN = 'WARN'
    INFO = 'INFO'
    ERROR = 'ERROR'


class BucketTweets(BaseModel):
    bucket: datetime.datetime
    tweet_count: int
    user_count: int
    user_tweets: dict[str, list[str]] | None


def nan_helper(y):
    # https://stackoverflow.com/questions/6518811/interpolate-nan-values-in-a-numpy-array
    return np.isnan(y), lambda z: z.nonzero()[0]


def main(target_dir: str | None = None,
         start_time: str = '2010-01-01 00:00',
         end_time: str = '2022-12-31 23:59',
         bucket_size: str = ['1 week', '2 weeks', '1 month', '3 months'][0],
         show: bool = False,
         export_png: bool = True,
         export_pdf: bool = False,
         export_svg: bool = False,
         export_html: bool = False,
         project_id: str | None = None,
         bot_annotation_tech: str = 'fc73da56-9f51-4d2b-ad35-2a01dbe9b275',
         bot_annotation_senti: str = 'e63da0c9-9bb5-4026-ab5e-7d5845cdc111',
         vector_file_basename: str | None = None,
         smoothing_windowsize: int | None = None,
         lookback_windowsize: str = '6 months',
         skip_cache: bool = False,
         n_cols: int = 2,
         log_level: LogLevel = LogLevel.DEBUG):
    if target_dir is None:
        target_dir = Path(settings.DATA_FIGURES) / 'wandering_points'
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
    logger = logging.getLogger('histograms')
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

    def fetch_bucketed_tweets(technology: int | None = None) -> list[BucketTweets]:
        tech_filter = 'AND ba_tech.value_int = :technology' if technology is not None else ''
        query = text(f"""
            WITH buckets as (SELECT generate_series(:start_time ::timestamp,
                                                    :end_time ::timestamp,
                                                    :bucket_size) as bucket),
                 labels as (SELECT DISTINCT ON (twitter_item.twitter_id, ba_tech.value_int) twitter_item.created_at,
                                                                                            twitter_item.twitter_author_id,
                                                                                            twitter_item.twitter_id,
                                                                                            twitter_item.item_id
                            FROM twitter_item
                                     LEFT JOIN bot_annotation ba_tech on (
                                        twitter_item.item_id = ba_tech.item_id
                                    AND ba_tech.bot_annotation_metadata_id = :ba_tech
                                    AND ba_tech.key = 'tech')
                            WHERE twitter_item.project_id = :project_id
                              {tech_filter}),
                 grouped as (SELECT b.bucket,
                                    l.twitter_author_id,
                                    array_agg(l.item_id)      as item_ids,
                                    count(DISTINCT l.twitter_id) as num_tweets
                             FROM buckets b
                                      LEFT JOIN labels l ON (
                                         l.created_at >= b.bucket
                                     AND l.created_at < (b.bucket + :bucket_size ::interval))
                             GROUP BY b.bucket, l.twitter_author_id
                             ORDER BY num_tweets)
            SELECT g.bucket,
                   SUM(g.num_tweets) as tweet_count,
                   COUNT(DISTINCT g.twitter_author_id) as user_count,
                   COALESCE(json_object_agg(g.twitter_author_id, g.item_ids)
                            FILTER (WHERE g.twitter_author_id IS NOT NULL), 'null'::JSON) as user_tweets
            FROM grouped g
            group by g.bucket
            ORDER BY g.bucket;
            """)

        def row_to_object(row) -> BucketTweets:
            return BucketTweets.parse_obj(row)

        result = query_cache.query_parsed(query, {
            'project_id': project_id,
            'ba_tech': bot_annotation_tech,
            'start_time': start_time,
            'end_time': end_time,
            'bucket_size': bucket_size,
            'technology': technology,
        }, row_to_object)
        return result

    logger.info(f'Loading index from {vector_file_basename}')
    index = VectorIndex()
    index.load(vector_file_basename)
    logger.debug('Building reverse lookup...')
    id2idx = {item_id: idx for idx, item_id in index.dict_labels.items()}
    vectors = index.vectors

    def plot_trace(buckets: list[BucketTweets], title: str):
        logger.debug('Computing centroids...')
        centroids = []
        for bucket in buckets:
            if bucket.user_tweets is not None:
                idxs = [id2idx[tid] for tids in bucket.user_tweets.values() for tid in tids]
                centroids.append(np.mean(vectors[idxs], axis=0))
            else:
                centroids.append(np.zeros((2,)))
        centroids = np.array(centroids)
        logger.debug('Gathering meta-data...')
        timestamps = [bucket.bucket for bucket in buckets]
        num_users = [bucket.user_count for bucket in buckets]
        num_tweets = [bucket.tweet_count for bucket in buckets]
        fig: plt.Figure
        ax: mpla.Axes
        fig, ax = plt.subplots(figsize=(10, 10), dpi=150)

        ax.scatter(vectors[:, 0], vectors[:, 1], alpha=0.3, s=3)

        ax.plot(centroids[:, 0], centroids[:, 1], 'o-', color='r', linewidth=1, markersize=5)
        # ax.scatter(centroids[:, 0], centroids[:, 1], s=10)#), s=num_tweets)

        fig.suptitle(title)
        fig.tight_layout()
        return fig

    def plot_traces(agg_buckets: list[list[BucketTweets]], titles: list[str]):
        fig: plt.Figure
        ax: mpla.Axes
        fig, ax = plt.subplots(figsize=(15, 15), dpi=150)

        # background scatter of landscape
        ax.scatter(vectors[:, 0], vectors[:, 1], alpha=0.05, s=1, c='grey')

        for title, buckets in zip(titles, agg_buckets):
            centroids = []
            for bucket in buckets:
                if bucket.user_tweets is not None:
                    idxs = [id2idx[tid] for tids in bucket.user_tweets.values() for tid in tids]
                    centroids.append(np.mean(vectors[idxs], axis=0))
                else:
                    # deliberately add NaN to be later filled by neighbour interpolation
                    centroids.append(np.zeros((2,)) * np.nan)

            centroids = np.array(centroids)
            nans, x = nan_helper(centroids[:, 0])
            centroids[:, 0][nans] = np.interp(x(nans), x(~nans), centroids[:, 0][~nans])
            nans, x = nan_helper(centroids[:, 1])
            centroids[:, 1][nans] = np.interp(x(nans), x(~nans), centroids[:, 1][~nans])

            # ax.plot(centroids[:, 0], centroids[:, 1], 'o-', linewidth=1, markersize=5, label=title)
            ax.plot(centroids[:, 0], centroids[:, 1], linewidth=1, label=title)
        ax.legend(bbox_to_anchor=(1.04, 1), borderaxespad=0)
        fig.suptitle(f'Aggregation level: "{bucket_size}"')
        fig.tight_layout()
        return fig

    def plot_traces_gif(agg_buckets: list[list[BucketTweets]], titles: list[str], trace_len: int = 12):
        fig: plt.Figure
        ax: mpla.Axes

        traces = {}

        for title, buckets in zip(titles, agg_buckets):
            centroids = []
            for bucket in buckets:
                if bucket.user_tweets is not None:
                    idxs = [id2idx[tid] for tids in bucket.user_tweets.values() for tid in tids]
                    centroids.append(np.mean(vectors[idxs], axis=0))
                else:
                    # deliberately add NaN to be later filled by neighbour interpolation
                    centroids.append(np.zeros((2,)) * np.nan)
            centroids = np.array(centroids)
            nans, x = nan_helper(centroids[:, 0])
            centroids[:, 0][nans] = np.interp(x(nans), x(~nans), centroids[:, 0][~nans])
            nans, x = nan_helper(centroids[:, 1])
            centroids[:, 1][nans] = np.interp(x(nans), x(~nans), centroids[:, 1][~nans])
            traces[title] = centroids

        timestamps = [bucket.bucket for bucket in agg_buckets[0]]

        fig, ax = plt.subplots(figsize=(15, 15), dpi=150)

        def animate(i):
            logger.debug(f'Processing frame {i}/{len(timestamps)}')
            si = max(0, i - trace_len)
            ei = i

            ax.clear()
            ax.set_xlim(-80, 80)
            ax.set_ylim(-70, 80)

            ret = []

            # background scatter of landscape
            ret.append(ax.scatter(vectors[:, 0], vectors[:, 1], alpha=0.05, s=1, c='grey'))


            for tech, trace in traces.items():
                line, = ax.plot(trace[si:ei][:, 0], trace[si:ei][:, 1], linewidth=1, label=title)
                line_end, = ax.plot(trace[ei][0], trace[ei][1], 'o', color=line.get_color())
                ret.append(line)
                ret.append(line_end)

            ax.legend(bbox_to_anchor=(1.04, 1), borderaxespad=0)
            fig.suptitle(f'From: {timestamps[si]} to {timestamps[ei]}')
            fig.tight_layout()
            return tuple(ret)
            # return line, line2, line3, point1, point2, point3,

        ani = FuncAnimation(fig, animate, interval=200, blit=True, repeat=True, frames=len(timestamps))
        ani.save(target_dir / 'animated.gif', dpi=150, writer=PillowWriter(fps=25))


    # logger.info(f'Fetching data for all')
    # data = fetch_bucketed_tweets(None)
    # figure = plot_trace(data, 'All tweets')
    # figure.show()

    joined = []
    for ti, tech_name in enumerate(queries.keys()):
        logger.info(f'Fetching data for {ti} "{tech_name}"')
        data = fetch_bucketed_tweets(ti)
        joined.append(data)
        # figure = plot_trace(data, tech_name)
        # figure.show()

    # figure = plot_traces(joined, list(queries.keys()))
    # figure.show()

    plot_traces_gif(joined, list(queries.keys()), trace_len=12)


if __name__ == "__main__":
    typer.run(main)
