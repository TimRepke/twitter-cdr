import logging
from enum import Enum
from pathlib import Path
import datetime
from typing import TypedDict

import typer
from pydantic import BaseModel
from sqlalchemy import text
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
import matplotlib.dates as mdates
import plotly.graph_objects as go

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


TechnologyCounts = TypedDict('TechnologyCounts', {
    'Methane Removal': int,
    'CCS': int,
    'Ocean Fertilization': int,
    'Ocean Alkalinization': int,
    'Enhanced Weathering': int,
    'Biochar': int,
    'Afforestation/Reforestation': int,
    'Ecosystem Restoration': int,
    'Soil Carbon Sequestration': int,
    'BECCS': int,
    'Blue Carbon': int,
    'Direct Air Capture': int,
    'GGR (general)': int
})

SentimentCounts = TypedDict('SentimentCounts', {
    'Negative': int,
    'Neutral': int,
    'Positive': int
})


class UserTweetCounts(BaseModel):
    twitter_author_id: str
    username: str
    num_cdr_tweets: int
    num_orig_cdr_tweets: int
    num_cdr_tweets_noccs: int
    num_orig_cdr_tweets_noccs: int
    num_tweets: int
    perc_orig: float
    perc_cdr: float
    tweet_count: int
    listed_count: int
    followers_count: int
    following_count: int
    name: str | None
    location: str | None
    earliest_cdr_tweet: datetime.datetime
    latest_cdr_tweet: datetime.datetime
    earliest_cdr_tweet_noccs: datetime.datetime | None
    latest_cdr_tweet_noccs: datetime.datetime | None
    time_cdr_active: datetime.timedelta
    time_to_first_cdr: datetime.timedelta
    created_at: datetime.datetime
    verified: bool
    description: str
    technologies: TechnologyCounts
    sentiments: SentimentCounts


def main(target_dir: str | None = None,
         start_time: str = '2006-01-01 00:00',
         end_time: str = '2022-12-31 23:59',
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
        target_dir = Path(settings.DATA_FIGURES) / 'exclusion'
    else:
        target_dir = Path(target_dir)

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

    def fetch_user_stats() -> list[UserTweetCounts]:
        query = text("""
                WITH user_tweets as (SELECT ti.item_id,
                                            ti.twitter_id,
                                            ti.twitter_author_id,
                                            ti.created_at                 as tweet_timestamp,
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
                                     WHERE ti.project_id = :project_id
                                       AND ti.created_at >= :start ::timestamp
                                       AND ti.created_at <= :end ::timestamp)
                SELECT ut.twitter_author_id,
                       ut.username,
                       -- Number of tweets matching any CDR query
                       count(DISTINCT ut.twitter_id)                                                  as num_cdr_tweets,
                       -- Number of tweets matching any CDR query (excluding Methane Removal (0) and CCS (1) )
                       count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int > 1)                  as num_cdr_tweets_noccs,
                
                       -- Tweets that are actually written and not just retweeted or quoted
                       count(DISTINCT ut.twitter_id) FILTER ( WHERE ut.is_orig )                      as num_orig_cdr_tweets,
                       -- Tweets that are actually written and not just retweeted or quoted (excluding Methane Removal (0) and CCS (1) )
                       count(DISTINCT ut.twitter_id) FILTER ( WHERE ut.is_orig AND ba.value_int > 1 ) as num_orig_cdr_tweets_noccs,
                       -- Total number of tweets by the user (as per Twitters profile information)
                       ut.tweet_count                                                                 as num_tweets,
                       (count(DISTINCT ut.twitter_id) FILTER ( WHERE ut.is_orig ))::float /
                       count(DISTINCT ut.twitter_id)::float * 100                                     as perc_orig,
                       count(DISTINCT ut.twitter_id)::float / ut.tweet_count::float * 100             as perc_cdr,
                       count(DISTINCT ut.twitter_id) FILTER (WHERE ba_senti.value_int = 2)            as "Positive",
                       count(DISTINCT ut.twitter_id) FILTER (WHERE ba_senti.value_int = 1)            as "Neutral",
                       count(DISTINCT ut.twitter_id) FILTER (WHERE ba_senti.value_int = 0)            as "Negative",
                       count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 0)                  as "Methane Removal",
                       count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 1)                  as "CCS",
                       count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 2)                  as "Ocean Fertilization",
                       count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 3)                  as "Ocean Alkalinization",
                       count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 4)                  as "Enhanced Weathering",
                       count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 5)                  as "Biochar",
                       count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 6)                  as "Afforestation/Reforestation",
                       count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 7)                  as "Ecosystem Restoration",
                       count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 8)                  as "Soil Carbon Sequestration",
                       count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 9)                  as "BECCS",
                       count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 10)                 as "Blue Carbon",
                       count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 11)                 as "Direct Air Capture",
                       count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 12)                 as "GGR (general)",
                       ut.tweet_count,
                       ut.listed_count,
                       ut.followers_count,
                       ut.following_count,
                       ut.name,
                       ut.location,
                       min(ut.tweet_timestamp)                                                        as earliest_cdr_tweet,
                       max(ut.tweet_timestamp)                                                        as latest_cdr_tweet,
                       max(ut.tweet_timestamp) - min(ut.tweet_timestamp)                              as time_cdr_active,
                       min(ut.tweet_timestamp) - ut.created_at                                        as time_to_first_cdr,
                       min(ut.tweet_timestamp) FILTER (WHERE ba.value_int >1)                         as earliest_cdr_tweet_noccs,
                       max(ut.tweet_timestamp) FILTER (WHERE ba.value_int >1)                         as latest_cdr_tweet_noccs,
                       ut.created_at,
                       ut.verified,
                       ut.description
                FROM user_tweets ut
                         LEFT JOIN bot_annotation ba_senti ON (
                            ut.item_id = ba_senti.item_id
                        AND ba_senti.bot_annotation_metadata_id = :ba_senti
                        AND ba_senti.key = 'senti'
                        AND ba_senti.repeat = 1)
                         LEFT JOIN bot_annotation ba ON (
                            ut.item_id = ba.item_id
                        AND ba.bot_annotation_metadata_id = :ba_tech
                        AND ba.key = 'tech'
                    )
                GROUP BY ut.name, ut.username, ut.location, ut.tweet_count, ut.listed_count, ut.followers_count,
                         ut.following_count,
                         ut.created_at, ut.verified, ut.description, ut.twitter_author_id;
            """)
        result = query_cache.query(query, {
            'project_id': project_id,
            'ba_tech': bot_annotation_tech,
            'ba_senti': bot_annotation_senti,
            'start': start_time,
            'end': end_time
        })

        senti_fields = set(SentimentCounts.__annotations__.keys())
        tech_fields = set(TechnologyCounts.__annotations__.keys())

        def row_to_obj(row) -> UserTweetCounts:
            user_data = {}
            tech_counts = {}
            senti_counts = {}
            for key, value in row.items():
                if key in tech_fields:
                    tech_counts[key] = value
                elif key in senti_fields:
                    senti_counts[key] = value
                else:
                    user_data[key] = value
            return UserTweetCounts(**user_data,
                                   technologies=TechnologyCounts(**tech_counts),
                                   sentiments=SentimentCounts(**senti_counts))

        return [row_to_obj(r) for r in result]

    def ratio_filter_fig():
        n_tweets = np.array([d.num_tweets for d in data])
        n_cdr_tweets = np.array([d.num_cdr_tweets for d in data])
        n_cdr_tweets_noccs = np.array([d.num_cdr_tweets_noccs for d in data])

        ratios = (n_cdr_tweets / n_tweets) * 100
        masks = [(ratios >= r) & (ratios < (r + 1)) for r in range(0, 101)]
        masks_cum = [ratios >= r for r in range(0, 101)]
        ratios_noccs = (n_cdr_tweets_noccs / n_tweets) * 100
        masks_noccs = [(ratios_noccs >= r) & (ratios_noccs < (r + 1)) for r in range(0, 101)]
        masks_cum_noccs = [ratios >= r for r in range(0, 101)]

        fig: plt.Figure
        axes: list[list[plt.Axes]]
        fig, axes = plt.subplots(2, 2, figsize=(15, 15), dpi=150)  # width/height
        fig.suptitle('Ratio of CDR tweets by a user (vs other tweets by that user)')

        axes[0][0].set_title('Number of users (incl. CCS & MR)')
        axes[0][0].bar(np.arange(0, 101), [sum(m) for m in masks])
        axes[0][0].plot(np.arange(0, 101), [sum(m) for m in masks_cum])

        axes[0][1].set_title('Number of tweets (incl. CCS & MR)')
        axes[0][1].bar(np.arange(0, 101), [n_cdr_tweets[m].sum() for m in masks])
        axes[0][1].plot(np.arange(0, 101), [n_cdr_tweets[m].sum() for m in masks_cum])

        axes[1][0].set_title('Number of users (excl. CCS & MR)')
        axes[1][0].bar(np.arange(0, 101), [sum(m) for m in masks])
        axes[1][0].plot(np.arange(0, 101), [sum(m) for m in masks_cum_noccs])

        axes[1][1].set_title('Number of tweets (excl. CCS & MR)')
        axes[1][1].bar(np.arange(0, 101), [n_cdr_tweets_noccs[m].sum() for m in masks_noccs])
        axes[1][1].plot(np.arange(0, 101), [n_cdr_tweets_noccs[m].sum() for m in masks_cum_noccs])

        fig.tight_layout()
        return fig

    def min_max(min_cdr: list[int], max_total: list[int]):
        n_tweets = np.array([d.num_tweets for d in data])
        n_cdr_tweets = np.array([d.num_cdr_tweets for d in data])
        n_cdr_tweets_noccs = np.array([d.num_cdr_tweets_noccs for d in data])

        user_cnt = np.zeros((len(min_cdr), len(max_total)))
        tweet_cnt = np.zeros((len(min_cdr), len(max_total)))
        user_cnt_noccs = np.zeros((len(min_cdr), len(max_total)))
        tweet_cnt_noccs = np.zeros((len(min_cdr), len(max_total)))

        for i, mc in enumerate(min_cdr):
            for j, mt in enumerate(max_total):
                mask = (n_tweets < mt) & (n_cdr_tweets > mc)
                user_cnt[i][j] = sum(mask)
                tweet_cnt[i][j] = n_cdr_tweets[mask].sum()

                mask = (n_tweets < mt) & (n_cdr_tweets_noccs > mc)
                user_cnt_noccs[i][j] = sum(mask)
                tweet_cnt_noccs[i][j] = n_cdr_tweets_noccs[mask].sum()

        logger.info(f'Number of users filtered by min num of CDR tweets:'
                    f' {min_cdr}; and max number of all tweets: {max_total}')
        logger.info(user_cnt)
        logger.info('... percentage of all users')
        logger.info(user_cnt / len(data))
        logger.info('... based on tweet counts excl MR / CCS')
        logger.info(user_cnt_noccs)
        logger.info('... percentage of all users')
        logger.info(user_cnt_noccs / len(data))

        logger.info(tweet_cnt)
        logger.info(tweet_cnt_noccs)
        logger.info(user_cnt_noccs / n_cdr_tweets_noccs.sum())

    def min_max_tpd(min_cdr: list[int], max_tpd: list[int]):

        n_tweets = np.array([d.num_tweets for d in data])
        lifetime = np.array([(datetime.datetime.now() - d.created_at).days for d in data])
        tpd = n_tweets / lifetime  # normalised tweets per day by account lifetime (note, that time between first and last tweet would be better metric)
        n_cdr_tweets = np.array([d.num_cdr_tweets for d in data])
        n_cdr_tweets_noccs = np.array([d.num_cdr_tweets_noccs for d in data])

        user_cnt = np.zeros((len(min_cdr), len(max_tpd)))
        tweet_cnt = np.zeros((len(min_cdr), len(max_tpd)))
        user_cnt_noccs = np.zeros((len(min_cdr), len(max_tpd)))
        tweet_cnt_noccs = np.zeros((len(min_cdr), len(max_tpd)))

        for i, mc in enumerate(min_cdr):
            for j, mt in enumerate(max_tpd):
                mask = (n_cdr_tweets >= mc) & (tpd <= mt)
                user_cnt[i][j] = sum(mask)
                tweet_cnt[i][j] = n_cdr_tweets[mask].sum()

                mask = (n_cdr_tweets_noccs >= mc) & (tpd <= mt)
                user_cnt_noccs[i][j] = sum(mask)
                tweet_cnt_noccs[i][j] = n_cdr_tweets_noccs[mask].sum()

        print(f'Number of users filtered by min num of CDR tweets:'
              f' {min_cdr}; and max number of tweets per day: {tpd}')
        print(pd.DataFrame(user_cnt, columns=max_tpd, index=min_cdr, dtype=int))
        print('... percentage of all users')
        print(pd.DataFrame(user_cnt / len(data), columns=max_tpd, index=min_cdr))
        print('... based on tweet counts excl MR / CCS')
        print(pd.DataFrame(user_cnt_noccs, columns=max_tpd, index=min_cdr, dtype=int))
        print('... percentage of all users')
        print(pd.DataFrame(user_cnt_noccs / len(data), columns=max_tpd, index=min_cdr))

        print('Number of tweets when users are filtered by their min number '
              'of CDR tweets and max number of tweets per day')
        print(pd.DataFrame(tweet_cnt, columns=max_tpd, index=min_cdr, dtype=int))
        print('... percentage of all CDR tweets')
        print(pd.DataFrame(tweet_cnt / n_cdr_tweets.sum(), columns=max_tpd, index=min_cdr))
        print('... excluding MR & CCS')
        print(pd.DataFrame(tweet_cnt_noccs, columns=max_tpd, index=min_cdr, dtype=int))
        print('... percentage of CDR tweets (excl MR & CCS)')
        print(pd.DataFrame(tweet_cnt_noccs / n_cdr_tweets_noccs.sum(), columns=max_tpd, index=min_cdr))

    def figure_min_max_tpd(min_cdr: list[int], max_tpd: list[int]):
        n_tweets = np.array([d.num_tweets for d in data])
        lifetime = np.array([(datetime.datetime(2022, 12, 31, 23, 59) - d.created_at).days for d in data])
        # normalised tweets per day by account lifetime
        # (note, that time between first and last tweet would be better metric)
        tpd = n_tweets / lifetime
        n_cdr_tweets = np.array([d.num_cdr_tweets for d in data])
        n_cdr_tweets_noccs = np.array([d.num_cdr_tweets_noccs for d in data])

        user_cnt = np.zeros((len(min_cdr), len(max_tpd)))
        tweet_cnt = np.zeros((len(min_cdr), len(max_tpd)))
        user_cnt_noccs = np.zeros((len(min_cdr), len(max_tpd)))
        tweet_cnt_noccs = np.zeros((len(min_cdr), len(max_tpd)))

        for i, mc in enumerate(min_cdr):
            for j, mt in enumerate(max_tpd):
                mask = (n_cdr_tweets >= mc) & (tpd <= mt)
                user_cnt[i][j] = sum(mask)
                tweet_cnt[i][j] = n_cdr_tweets[mask].sum()

                mask = (n_cdr_tweets_noccs >= mc) & (tpd <= mt)
                user_cnt_noccs[i][j] = sum(mask)
                tweet_cnt_noccs[i][j] = n_cdr_tweets_noccs[mask].sum()

        fig: plt.Figure
        axes: list[list[plt.Axes]]
        fig, axes = plt.subplots(2, 2, figsize=(15, 15), dpi=150)  # width/height

        axes[0][0].set_title('Number of users')
        rel = user_cnt / len(data)
        axes[0][0].imshow(rel, cmap='YlOrRd')
        for i in range(len(min_cdr)):
            for j in range(len(max_tpd)):
                axes[0][0].text(j, i, f'{rel[i, j]:.2f}', ha='center', va='center', size=5)

        axes[0][1].set_title('Number of tweets')
        rel = tweet_cnt / n_cdr_tweets.sum()
        axes[0][1].imshow(rel, cmap='YlOrRd')
        for i in range(len(min_cdr)):
            for j in range(len(max_tpd)):
                axes[0][1].text(j, i, f'{rel[i, j]:.2f}', ha='center', va='center', size=5)

        axes[1][0].set_title('Number of users (excl MR & CCS)')
        rel = user_cnt_noccs / len(data)
        axes[1][0].imshow(rel, cmap='YlOrRd')
        for i in range(len(min_cdr)):
            for j in range(len(max_tpd)):
                axes[1][0].text(j, i, f'{rel[i, j]:.2f}', ha='center', va='center', size=5)

        axes[1][1].set_title('Number of tweets (excl MR & CCS)')
        rel = tweet_cnt_noccs / n_cdr_tweets_noccs.sum()
        axes[1][1].imshow(rel, cmap='YlOrRd')
        for i in range(len(min_cdr)):
            for j in range(len(max_tpd)):
                axes[1][1].text(j, i, f'{rel[i, j]:.2f}', ha='center', va='center', size=5)

        for i in range(2):
            for j in range(2):
                axes[i][j].xaxis.tick_top()
                axes[i][j].set_xticks(range(len(max_tpd)))
                axes[i][j].set_xticklabels(max_tpd, rotation=90)
                axes[i][j].set_yticks(range(len(min_cdr)))
                axes[i][j].set_yticklabels(min_cdr)
        return fig

    logger.info('Fetching tweet counts per technology of top users...')
    data = fetch_user_stats()

    logger.info('CDR tweet ratio filter...')
    figure = ratio_filter_fig()
    show_save(figure, target=target_dir / 'ratio_filters')

    logger.info('CDR tweet min CDR / max overall filter...')
    min_max(min_cdr=[2, 3, 4, 5, 10, 20, 50],
            max_total=[1000000, 100000, 10000])
    min_max_tpd(min_cdr=[1, 2, 3, 4, 5, 10, 20, 50, 100, 500, 1000],
                max_tpd=[1, 2, 5, 10, 20, 50, 100, 500, 1000])

    figure = figure_min_max_tpd(min_cdr=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 30, 40, 50, 60, 70, 80, 90,
                                         100, 200, 300, 400, 500, 600, 700, 800, 900, 1000],
                                max_tpd=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 30, 40, 50, 60, 70, 80, 90,
                                         100, 200, 300, 400, 500, 600, 700, 800, 900, 1000])
    show_save(figure, target=target_dir / 'tpd_filter')


if __name__ == "__main__":
    typer.run(main)
