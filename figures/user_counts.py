import inspect
import logging
from enum import Enum
from pathlib import Path
from datetime import datetime
from typing import Sequence, Any

import typer
from pydantic import BaseModel
from sqlalchemy import text, TextClause, RowMapping
from sqlalchemy.orm import Session
import numpy as np
from matplotlib import pyplot as plt
import matplotlib.dates as mdates

from nacsos_data.db import DatabaseEngine

from common.config import settings
from common.db_cache import QueryCache
from common.queries import queries


class Resolution(str, Enum):
    hour = 'hour'
    day = 'day'
    week = 'week'
    biweek = 'biweek'
    month = 'month'
    quarter = 'quarter'
    year = 'year'


class LogLevel(str, Enum):
    DEBUG = 'DEBUG'
    WARN = 'WARN'
    INFO = 'INFO'
    ERROR = 'ERROR'


class CumCount(BaseModel):
    bucket: datetime
    cum_tweets: int
    cum_users: int
    tpu: float | None


class UserTweetCounts(BaseModel):
    twitter_author_id: str
    username: str
    num_cdr_tweets: int
    num_orig_cdr_tweets: int
    tweet_count: int
    listed_count: int
    followers_count: int
    following_count: int
    name: str | None
    location: str | None
    earliest_cdr_tweet: datetime
    latest_cdr_tweet: datetime
    created_at: datetime
    verified: bool
    description: str
    counts: dict[str, int]


def main(target_dir: str | None = None,
         start_time: str = '2010-01-01 00:00',
         end_time: str = '2022-12-31 23:59',
         resolution: Resolution = Resolution.day,
         show: bool = False,
         export_png: bool = True,
         export_pdf: bool = False,
         export_svg: bool = False,
         project_id: str | None = None,
         bot_annotation_tech: str = 'fc73da56-9f51-4d2b-ad35-2a01dbe9b275',
         bot_annotation_senti: str = 'e63da0c9-9bb5-4026-ab5e-7d5845cdc111',
         smoothing_windowsize: int | None = None,
         lookback_windowsize: str = '6 months',
         skip_cache: bool = False,
         n_cols: int = 2,
         log_level: LogLevel = LogLevel.DEBUG):
    if target_dir is None:
        target_dir = Path(settings.DATA_FIGURES) / 'user_counts'
    else:
        target_dir = Path(target_dir)

    if project_id is None:
        project_id = settings.PROJECT_ID

    interval = {
        Resolution.day: '1 day',
        Resolution.week: '1 week',
        Resolution.biweek: '2 weeks',
        Resolution.month: '1 month',
        Resolution.quarter: '3 months',
        Resolution.year: '1 year'
    }[resolution]

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

    def smooth(array, with_pad=True) -> np.ndarray:
        if smoothing_windowsize is None:
            return array

        kernel = np.ones(smoothing_windowsize) / smoothing_windowsize

        if with_pad:
            padded = [np.pad(row, smoothing_windowsize // 2, mode='edge') for row in array]
            smoothed = [np.convolve(row, kernel, mode='same') for row in padded]
            return np.array(smoothed).T[smoothing_windowsize // 2:-smoothing_windowsize // 2].T

        return np.array([np.convolve(row, kernel, mode='valid') for row in array])

    def fetch_user_tweet_counts() -> list[UserTweetCounts]:
        query = text("""
            WITH user_tweets as (SELECT ti.item_id,
                                        ti.twitter_id,
                                        ti.twitter_author_id,
                                        u.tweet_count,
                                        u.listed_count,
                                        u.followers_count,
                                        u.following_count,
                                        u.name,
                                        u.username,
                                        u.location,
                                        u.created_at,
                                        u.verified,
                                        u.description,
                                        ti.referenced_tweets = 'null' as is_orig
                                 FROM twitter_item ti,
                                      jsonb_to_record(ti."user") as u (
                                                                       "name" text,
                                                                       "username" text,
                                                                       "location" text,
                                                                       "tweet_count" int,
                                                                       "listed_count" int,
                                                                       "followers_count" int,
                                                                       "following_count" int,
                                                                       "created_at" timestamp,
                                                                       "verified" bool,
                                                                       "description" text
                                          )
                                 WHERE ti.project_id = :project_id)
            SELECT ut.twitter_author_id,
                   ut.username,
                   -- Number of tweets matching any CDR query
                   count(DISTINCT ut.twitter_id)                                  as num_cdr_tweets,
                   -- Tweets that are actually written and not just retweeted or quoted
                   count(DISTINCT ut.twitter_id) FILTER ( WHERE ut.is_orig )      as num_orig_cdr_tweets,
                   -- Total number of tweets by the user (as per Twitters profile information)
                   count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 0)  as "Methane removal",
                   count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 1)  as "CCS",
                   count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 2)  as "Ocean fertilization",
                   count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 3)  as "Ocean alkalinization",
                   count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 4)  as "Enhanced weathering",
                   count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 5)  as "Biochar",
                   count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 6)  as "Afforestation/reforestation",
                   count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 7)  as "Ecosystem restoration",
                   count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 8)  as "Soil carbon sequestration",
                   count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 9)  as "BECCS",
                   count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 10) as "Blue carbon",
                   count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 11) as "Direct air capture",
                   count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 12) as "GGR (general)",
                   ut.tweet_count,
                   ut.listed_count,
                   ut.followers_count,
                   ut.following_count,
                   ut.name,
                   ut.location,
                   min(ut.created_at)                                             as earliest_cdr_tweet,
                   max(ut.created_at)                                             as latest_cdr_tweet,
                   ut.created_at,
                   ut.verified,
                   ut.description
            FROM user_tweets ut
                     LEFT JOIN bot_annotation ba on (ut.item_id = ba.item_id
                AND ba.bot_annotation_metadata_id = :ba_tech
                AND ba.key = 'tech')
            GROUP BY ut.name, ut.username, ut.location, ut.tweet_count, ut.listed_count, ut.followers_count, ut.following_count,
                     ut.created_at, ut.verified, ut.description, ut.twitter_author_id
            ORDER BY num_cdr_tweets DESC
            LIMIT :limit;
            """)
        result = query_cache.query(query, {
            'project_id': project_id,
            'ba_tech': bot_annotation_tech,
            'limit': 200
        })

        obj_fields = set(UserTweetCounts.schema()['properties'].keys())

        def row_to_obj(row) -> UserTweetCounts:
            cp_counts = dict(row)
            cp_data = dict(row)
            for key in row.keys():
                if key in obj_fields:
                    del cp_counts[key]
                else:
                    del cp_data[key]
            return UserTweetCounts(**cp_data, counts=cp_counts)

        return [row_to_obj(r) for r in result]

    def fetch_cumulative_counts() -> list[CumCount]:
        query = text("""
        WITH buckets as (SELECT generate_series(:start_time ::timestamp,
                                                :end_time ::timestamp,
                                                :resolution) as bucket)
        SELECT b.bucket                                                                           as bucket,
               count(DISTINCT ti.twitter_id)                                                      as cum_tweets,
               count(DISTINCT ti.twitter_author_id)                                               as cum_users,
               count(DISTINCT ti.twitter_id)::float / count(DISTINCT ti.twitter_author_id)::float as tpu
        FROM buckets b
                 LEFT JOIN twitter_item ti ON (
                    ti.project_id = :project_id
                AND ti.created_at < b.bucket)
        GROUP BY bucket
        ORDER BY bucket;
        """)
        result = query_cache.query(query, {
            'start_time': start_time,
            'end_time': end_time,
            'resolution': interval,
            'project_id': project_id
        })
        return [CumCount.parse_obj(r) for r in result]

    def fetch_cumulative_counts_window() -> list[CumCount]:
        query = text("""
        WITH buckets as (SELECT generate_series(:start_time ::timestamp,
                                                :end_time ::timestamp,
                                                :resolution) as bucket)
        SELECT b.bucket                                                                           as bucket,
               count(DISTINCT ti.twitter_id)                                                      as cum_tweets,
               count(DISTINCT ti.twitter_author_id)                                               as cum_users,
               count(DISTINCT ti.twitter_id)::float / count(DISTINCT ti.twitter_author_id)::float as tpu
        FROM buckets b
                 LEFT JOIN twitter_item ti ON (
                    ti.project_id = :project_id
                AND ti.created_at >= (b.bucket - :window_size ::interval)
                AND ti.created_at < b.bucket)
        GROUP BY bucket
        ORDER BY bucket;
        """)
        result = query_cache.query(query, {
            'start_time': start_time,
            'end_time': end_time,
            'resolution': interval,
            'window_size': lookback_windowsize,
            'project_id': project_id
        })
        return [CumCount.parse_obj(r) for r in result]

    def fetch_cumulative_counts_tech(technology: int) -> list[CumCount]:
        query = text("""
        WITH buckets as (SELECT generate_series(:start_time ::timestamp,
                                                :end_time ::timestamp,
                                                :resolution) as bucket),
             labels as (SELECT DISTINCT ON (twitter_item.twitter_id, ba_tech.value_int) twitter_item.created_at,
                                                                                        twitter_item.twitter_author_id,
                                                                                        twitter_item.twitter_id,
                                                                                        ba_tech.value_int as technology
                        FROM twitter_item
                                 LEFT OUTER JOIN bot_annotation ba_tech on (
                                    twitter_item.item_id = ba_tech.item_id
                                AND ba_tech.bot_annotation_metadata_id = :ba_tech
                                AND ba_tech.key = 'tech')
                        WHERE twitter_item.project_id = :project_id
                          AND ba_tech.value_int = :technology)
        SELECT b.bucket                                                                         as bucket,
               count(DISTINCT l.twitter_id)                                                     as cum_tweets,
               count(DISTINCT l.twitter_author_id)                                              as cum_users,
               count(DISTINCT l.twitter_id)::float / nullif(count(DISTINCT l.twitter_author_id)::float, 0) as tpu
        FROM buckets b
                 LEFT JOIN labels l ON (l.created_at < b.bucket)
        GROUP BY bucket
        ORDER BY bucket;
        """)
        result = query_cache.query(query, {
            'start_time': start_time,
            'end_time': end_time,
            'resolution': interval,
            'project_id': project_id,
            'ba_tech': bot_annotation_tech,
            'technology': technology
        })
        return [CumCount.parse_obj(r) for r in result]

    def fetch_cumulative_window_counts_tech(technology: int) -> list[CumCount]:
        query = text("""
        WITH buckets as (SELECT generate_series(:start_time ::timestamp,
                                                :end_time ::timestamp,
                                                :resolution) as bucket),
             labels as (SELECT DISTINCT ON (twitter_item.twitter_id, ba_tech.value_int) twitter_item.created_at,
                                                                                        twitter_item.twitter_author_id,
                                                                                        twitter_item.twitter_id,
                                                                                        ba_tech.value_int as technology
                        FROM twitter_item
                                 LEFT OUTER JOIN bot_annotation ba_tech on (
                                    twitter_item.item_id = ba_tech.item_id
                                AND ba_tech.bot_annotation_metadata_id = :ba_tech
                                AND ba_tech.key = 'tech')
                        WHERE twitter_item.project_id = :project_id
                          AND ba_tech.value_int = :technology)
        SELECT b.bucket                                                                         as bucket,
               count(DISTINCT l.twitter_id)                                                     as cum_tweets,
               count(DISTINCT l.twitter_author_id)                                              as cum_users,
               count(DISTINCT l.twitter_id)::float / nullif(count(DISTINCT l.twitter_author_id)::float, 0) as tpu
        FROM buckets b
                 LEFT JOIN labels l ON (
                        l.created_at >= (b.bucket - :window_size ::interval)
                    AND l.created_at < b.bucket)
        GROUP BY bucket
        ORDER BY bucket;
        """)
        result = query_cache.query(query, {
            'start_time': start_time,
            'end_time': end_time,
            'resolution': interval,
            'project_id': project_id,
            'ba_tech': bot_annotation_tech,
            'technology': technology,
            'window_size': lookback_windowsize
        })
        return [CumCount.parse_obj(r) for r in result]

    def plot_heatmap(counts: list[UserTweetCounts], relative: bool):
        users = [d.username for d in counts]
        technologies = list(counts[0].counts.keys())
        num_tweets = np.array([d.num_cdr_tweets for d in counts], dtype=float)
        num_tech = np.array([list(d.counts.values()) for d in counts], dtype=float).T

        fig, ax = plt.subplots(figsize=(8, 20), dpi=150)
        if relative:
            shares = np.divide(num_tech, num_tweets, out=np.zeros_like(num_tech), where=num_tweets != 0)
            im = ax.imshow(shares.T, cmap='YlOrRd')
        else:
            pass
        # We want to show all ticks...
        ax.set_xticks(np.arange(len(technologies)))
        ax.set_yticks(np.arange(len(users)))
        # ... and label them with the respective list entries
        ax.set_xticklabels(technologies, fontsize=6)
        ax.set_yticklabels(users, fontsize=6)

        # Rotate the tick labels and set their alignment.
        plt.setp(ax.get_xticklabels(), rotation=45, ha="right", rotation_mode="anchor")
        # fig.colorbar(im)
        fig.tight_layout()
        return fig

    def plot_cum(counts: list[CumCount]) -> plt.Figure:
        fig, ax = plt.subplots(nrows=1, ncols=2, figsize=(10, 5), dpi=150)
        dates = [c.bucket for c in counts]
        n_users = [c.cum_users for c in counts]
        n_tweets = [c.cum_tweets for c in counts]
        tpu = np.array([c.tpu for c in counts])

        ax[0].plot(dates, n_users, label='Number of users')
        ax[0].plot(dates, n_tweets, label='Number of tweets')
        ax[0].set_xlim(dates[0], dates[-1])
        ax[0].xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        ax[0].legend()

        ax[1].plot(dates, tpu, label='Tweets per user')
        ax[1].set_xlim(dates[0], dates[-1])
        ax[1].set_ylim(1, np.max(tpu))
        ax[1].xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        ax[1].legend()

        fig.autofmt_xdate()

        # box = ax[0].get_position()
        # ax[0].set_position([box.x0, box.y0, box.width * 0.8, box.height])
        # ax_handles, ax_labels = ax.get_legend_handles_labels()
        # ax.legend(ax_handles[::-1], ax_labels[::-1], loc='center left', bbox_to_anchor=(1, 0.5))
        fig.tight_layout()
        return fig

    def plot_cums(tech_counts: list[list[CumCount]], share_y: bool) -> plt.Figure:
        n_rows = int(np.ceil(len(queries) / n_cols))
        logger.info(f'Creating aggregate figure with {n_rows} rows and {n_cols} columns.')

        fig, axes = plt.subplots(n_rows, n_cols, figsize=(4 * n_cols, 3 * n_rows), dpi=150, sharey=share_y)

        technologies = list(queries.keys())
        dates = [c.bucket for c in tech_counts[0]]

        for i, (technology, counts) in enumerate(zip(technologies, tech_counts)):
            row = i // n_cols
            col = i % n_cols
            logger.debug(f' -> plotting {technology} to row {row}; column {col}')
            ax = axes[row][col]
            ax.set_title(technology)
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))

            n_users = [c.cum_users for c in counts]
            n_tweets = [c.cum_tweets for c in counts]
            tpu = np.array([c.tpu for c in counts], dtype=float)
            tpu = np.nan_to_num(tpu, nan=np.nanmedian(tpu))

            ax.plot(dates, n_users, label='Number of users')
            ax.plot(dates, n_tweets, label='Number of tweets')
            ax.set_xlim(dates[0], dates[-1])
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))

            subax = ax.twinx()
            subax.plot(dates, tpu, label='Tweets per user', color='r')
            subax.set_ylim(1, np.max(tpu))

            ax.legend()
            subax.legend()
        # axes[n_rows - 1][n_cols - 1].legend()
        fig.autofmt_xdate()

        # box = ax[0].get_position()
        # ax[0].set_position([box.x0, box.y0, box.width * 0.8, box.height])
        # ax_handles, ax_labels = ax.get_legend_handles_labels()
        # ax.legend(ax_handles[::-1], ax_labels[::-1], loc='center left', bbox_to_anchor=(1, 0.5))
        fig.tight_layout()
        return fig

    logger.info('Fetching tweet counts per technology of top users...')
    data = fetch_user_tweet_counts()
    figure = plot_heatmap(data, relative=True)
    show_save(figure, target_dir / 'technologies' / f'top_200_heatmap')

    logger.info('Fetching cumulative (sliding window) numbers...')
    data = fetch_cumulative_counts_window()
    figure = plot_cum(data)
    figure.suptitle(f'Tweets per user measured every {resolution.value} looking back {lookback_windowsize}')
    figure.tight_layout()
    show_save(figure,
              target_dir / 'cumulative' /
              f'lines_{resolution.value}_sliding_{lookback_windowsize.replace(" ", "")}')

    logger.info('Fetching cumulative numbers...')
    data = fetch_cumulative_counts()
    figure = plot_cum(data)
    figure.suptitle(f'Cumulative tweets/users resolution {resolution.value}')
    figure.tight_layout()
    show_save(figure, target_dir / 'cumulative' / f'lines_{resolution.value}')

    logger.info('Cumulative numbers per technology...')
    data_acc = []
    for ti, technology_name in enumerate(queries.keys()):
        logger.debug(f' - fetching for ({ti}) {technology_name}')
        data = fetch_cumulative_counts_tech(ti)
        data_acc.append(data)
    figure = plot_cums(data_acc, share_y=False)
    figure.suptitle(f'Cumulative tweets/users looking back each "{interval}"\n')
    figure.tight_layout()
    show_save(figure, target_dir / 'cumulative' / f'all_lines_{resolution.value}')

    logger.info('Cumulative numbers withing sliding window per technology...')
    data_acc = []
    for ti, technology_name in enumerate(queries.keys()):
        logger.debug(f' - fetching for ({ti}) {technology_name}')
        data = fetch_cumulative_window_counts_tech(ti)
        data_acc.append(data)
    figure = plot_cums(data_acc, share_y=False)
    figure.suptitle(f'Cumulative tweets/users looking back each "{interval}" for {lookback_windowsize}\n')
    figure.tight_layout()
    show_save(figure,
              target_dir / 'cumulative' /
              f'all_lines_{resolution.value}_sliding_{lookback_windowsize.replace(" ", "")}')


if __name__ == "__main__":
    typer.run(main)
