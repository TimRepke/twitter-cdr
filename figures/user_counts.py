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
         skip_cache: bool = False,
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

    def plot_heatmap(counts: list[UserTweetCounts], relative:bool):
        users = [d.username for d in counts]
        technologies = list(counts[0].counts.keys())
        num_tweets = np.array([d.num_cdr_tweets for d in counts], dtype=float)
        num_tech = np.array([list(d.counts.values()) for d in counts], dtype=float).T

        fig, ax = plt.subplots(figsize=(8,20), dpi=150)
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


    logger.info('Fetching tweet counts per technology of top users...')
    data = fetch_user_tweet_counts()
    figure = plot_heatmap(data, relative=True)
    figure.show()
    # show_save(figure, target_dir / 'sentiments_temporal' / f'tempo_{resolution.value}_abs_tech_all')


if __name__ == "__main__":
    typer.run(main)
