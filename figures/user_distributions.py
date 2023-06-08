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

from nacsos_data.db import DatabaseEngine

from common.config import settings
from common.db_cache import QueryCache
from common.queries import queries


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
    time_cdr_active: datetime.timedelta
    time_to_first_cdr: datetime.timedelta
    created_at: datetime.datetime
    verified: bool
    description: str
    technologies: TechnologyCounts
    sentiments: SentimentCounts


def main(target_dir: str | None = None,
         start_time: str = '2010-01-01 00:00',
         end_time: str = '2022-12-31 23:59',
         show: bool = False,
         export_png: bool = True,
         export_pdf: bool = False,
         export_svg: bool = False,
         export_html: bool = False,
         project_id: str | None = None,
         bot_annotation_tech: str = 'fc73da56-9f51-4d2b-ad35-2a01dbe9b275',
         bot_annotation_senti: str = 'e63da0c9-9bb5-4026-ab5e-7d5845cdc111',
         smoothing_windowsize: int | None = None,
         lookback_windowsize: str = '6 months',
         skip_cache: bool = False,
         n_cols: int = 2,
         log_level: LogLevel = LogLevel.DEBUG):
    if target_dir is None:
        target_dir = Path(settings.DATA_FIGURES) / 'user_distributions'
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

    def show_savely(fig: go.Figure, target: Path):
        target.parent.mkdir(parents=True, exist_ok=True)

        if export_png:
            fig.write_image(str(target) + '.png', format='png')
        if export_svg:
            fig.write_image(str(target) + '.svg', format='svg')
        if export_pdf:
            fig.write_image(str(target) + '.pdf', format='pdf')
        if export_html:
            fig.write_html(str(target) + '.html', auto_open=False)

        if show:
            fig.show()

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
                                 AND ti.created_at >= :start_time ::timestamp
                                 AND ti.created_at <= :end_time ::timestamp)
            SELECT ut.twitter_author_id,
                   MAX(ut.username)                                                               as username,
                   -- Number of tweets matching any CDR query
                   count(DISTINCT ut.twitter_id)                                                  as num_cdr_tweets,
                   -- Number of tweets matching any CDR query (excluding Methane Removal (0) and CCS (1) )
                   count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int > 1)                  as num_cdr_tweets_noccs,
            
                   -- Tweets that are actually written and not just retweeted or quoted
                   count(DISTINCT ut.twitter_id) FILTER ( WHERE ut.is_orig )                      as num_orig_cdr_tweets,
                   -- Tweets that are actually written and not just retweeted or quoted (excluding Methane Removal (0) and CCS (1) )
                   count(DISTINCT ut.twitter_id) FILTER ( WHERE ut.is_orig AND ba.value_int > 1 ) as num_orig_cdr_tweets_noccs,
                   -- Total number of tweets by the user (as per Twitters profile information)
                   MAX(ut.tweet_count)                                                            as num_tweets,
                   (count(DISTINCT ut.twitter_id) FILTER ( WHERE ut.is_orig ))::float /
                   count(DISTINCT ut.twitter_id)::float * 100                                     as perc_orig,
                   count(DISTINCT ut.twitter_id)::float / MAX(ut.tweet_count)::float * 100        as perc_cdr,
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
                   MAX(ut.tweet_count) as tweet_count,
                   MAX(ut.listed_count) as listed_count,
                   MAX(ut.followers_count) as followers_count,
                   MAX(ut.following_count) as following_count,
                   MAX(ut.name) as name,
                   MAX(ut.location) as location,
                   min(ut.tweet_timestamp)                                                        as earliest_cdr_tweet,
                   max(ut.tweet_timestamp)                                                        as latest_cdr_tweet,
                   max(ut.tweet_timestamp) - min(ut.tweet_timestamp)                              as time_cdr_active,
                   min(ut.tweet_timestamp) - MAX(ut.created_at)                                   as time_to_first_cdr,
                   min(ut.tweet_timestamp) FILTER (WHERE ba.value_int >1)                         as earliest_cdr_tweet_noccs,
                   max(ut.tweet_timestamp) FILTER (WHERE ba.value_int >1)                         as latest_cdr_tweet_noccs,
                   MAX(ut.created_at) as created_at,
                   bool_or(ut.verified) as verified,
                   MAX(ut.description) as description
            FROM user_tweets ut
                     LEFT JOIN bot_annotation ba_senti ON (
                        ut.item_id = ba_senti.item_id
                    AND ba_senti.bot_annotation_metadata_id = :ba_senti
                    AND ba_senti.key = 'senti'
                    AND ba_senti.repeat = 1)
                     LEFT JOIN bot_annotation ba ON (
                        ut.item_id = ba.item_id
                    AND ba.bot_annotation_metadata_id = :ba_tech
                    AND ba.key = 'tech')
            GROUP BY ut.twitter_author_id
            ORDER BY num_cdr_tweets DESC;
            """)

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

        result = query_cache.query_parsed(query, {
            'project_id': project_id,
            'ba_tech': bot_annotation_tech,
            'ba_senti': bot_annotation_senti,
            'start_time': start_time,
            'end_time': end_time
        }, row_to_obj)
        return result

    def plot_hist(x: np.ndarray, label: str, bins: int = 100, log: bool = False, density: bool = False,
                  cum: bool = False, xrange: tuple[int, int] | None = None):
        fig: plt.Figure
        ax: mpla.Axes
        fig, ax = plt.subplots()
        ax.hist(x, bins=bins, density=density, cumulative=cum, range=xrange,
                histtype='bar', log=log, label=label)

        # ax.set_yscale('log')
        if xrange is not None:
            ax.set_xlabel(f'Limited from ({x.min():,.0f}–{x.max():,.0f}) to ({xrange[0]:,}–{xrange[1]:,})')
        fig.suptitle(f'Histogram of {label} (n_users: {len(x):,}, \n'
                     f'max: {np.max(x):,.0f}, mean: {np.mean(x):,.1f}, median: {np.median(x):,.0f})')
        fig.tight_layout()
        return fig

    def plot_hist2d(x: np.ndarray, y: np.ndarray, label: tuple[str, str], bins: int = 100, density: bool = False,
                    vmin: float | None = None, vmax: float | None = None, colorbar: bool = False,
                    vlog: bool = False, xlog: bool = False, ylog: bool = False,
                    xrange: tuple[float, float] | None = None,
                    yrange: tuple[float, float] | None = None):
        fig: plt.Figure
        ax: mpla.Axes
        h: np.ndarray

        if xrange is None and yrange is None:
            xyrange = None
        else:
            xyrange = [[x.min(), x.max()] if xrange is None else xrange,
                       [y.min(), y.max()] if yrange is None else yrange]

        logger.debug(f'xmin={x.min()}, xmax={x.max()}, xmean={np.mean(x)}, xmedian={np.median(x)} |\n'
                     f'ymin={y.min()}, ymax={y.max()}, ymean={np.mean(y)}, ymedian={np.median(y)}')

        if xlog or ylog:
            binning = (np.logspace(np.nan_to_num(np.log10(x.min()), neginf=0), np.log10(x.max()), bins)
                       if xlog else np.linspace(x.min(), x.max(), bins),
                       np.logspace(np.nan_to_num(np.log10(y.min()), neginf=0), np.log10(y.max()), bins)
                       if ylog else np.linspace(y.min(), y.max(), bins))
        else:
            binning = bins

        fig, ax = plt.subplots()
        h, _, _, im = ax.hist2d(x, y, bins=binning, density=density, range=xyrange, label=label,
                                vmin=vmin, vmax=vmax, cmap='YlOrRd', norm='log' if vlog else 'linear')

        if xlog:
            ax.set_xscale('log')
        if ylog:
            ax.set_yscale('log')

        if xrange is not None:
            ax.set_xlabel(
                f'{label[0]} limited from ({x.min():,.0f}–{x.max():,.0f}) to ({xrange[0]:,}–{xrange[1]:,})')
        else:
            ax.set_xlabel(label[0])
        if yrange is not None:
            ax.set_ylabel(
                f'{label[1]} limited from ({y.min():,.0f}–{y.max():,.0f}) to ({yrange[0]:,}–{yrange[1]:,})')
        else:
            ax.set_ylabel(label[1])

        if colorbar:
            fig.colorbar(im)
        fig.suptitle(f'Histogram (vmin={h.min():,.1f}; vmax={h.max():,.1f})')
        fig.tight_layout()
        return fig

    logger.info('Fetching user stats...')
    data = fetch_user_tweet_counts()

    # figure = plot_hist(np.array([d.num_tweets for d in data]), log=True,
    #                    label='Tweets per user')
    # figure.show()
    # figure = plot_hist(np.array([d.num_cdr_tweets for d in data]), log=True,
    #                    label='CDR Tweets per user')
    # figure.show()
    # figure = plot_hist(np.array([d.num_cdr_tweets for d in data]), log=True, xrange=(0, 1000),
    #                    label='CDR Tweets per user')
    # figure.show()
    # figure = plot_hist(np.array([d.num_cdr_tweets for d in data]), log=False, cum=-1, xrange=(0, 20),
    #                    label='CDR Tweets per user')
    # figure.show()
    # figure = plot_hist(np.array([d.num_cdr_tweets for d in data]), log=False, cum=True, xrange=(0, 20),
    #                    label='CDR Tweets per user')
    # figure.show()
    # figure = plot_hist(np.array([d.num_orig_cdr_tweets for d in data]), log=True,
    #                    label='Original CDR Tweets per user')
    # figure.show()
    # figure = plot_hist(np.array([d.perc_cdr for d in data]), log=True,
    #                    label='CDR tweet ratio per user')
    # figure.show()
    # figure = plot_hist(np.array([d.perc_cdr for d in data]), log=False,
    #                    label='CDR tweet ratio per user')
    # figure.show()
    # figure = plot_hist(np.array([d.perc_cdr for d in data]), log=False, xrange=(2,100),
    #                    label='CDR tweet ratio per user')
    # figure.show()
    # figure = plot_hist(np.array([d.perc_cdr for d in data]), log=True, cum=-1,
    #                    label='CDR tweet ratio per user')
    # figure.show()
    figure = plot_hist(np.array([d.perc_orig for d in data]), log=True,
                       label='Original CDR tweet ratio per user')
    figure.show()
    figure = plot_hist(np.array([d.perc_orig for d in data]), log=False,
                       label='Original CDR tweet ratio per user')
    figure.show()
    figure = plot_hist(np.array([d.perc_orig for d in data]), log=False, xrange=(2, 98),
                       label='Original CDR tweet ratio per user')
    figure.show()
    # figure = plot_hist(np.array([d.time_cdr_active.days for d in data]),
    #                    label='Days btw first and last CDR tweet')
    # figure.show()
    # figure = plot_hist(np.array([d.time_cdr_active.days for d in data]), xrange=(0, 2000), log=True,
    #                    label='Days btw first and last CDR tweet')
    # figure.show()
    # figure = plot_hist(np.array([d.time_cdr_active.days for d in data]), log=True,
    #                    label='Days btw first and last CDR tweet')
    # figure.show()
    # figure = plot_hist(np.array([d.time_to_first_cdr.days for d in data]),
    #                    label='Days to first CDR tweet')
    # figure.show()
    # figure = plot_hist(np.array([(d.created_at - datetime.datetime(2006, 3, 21)).days for d in data]),
    #                    label='Account creation (days since Mar 2006)')
    # figure.show()
    #
    # figure = plot_hist2d(np.array([d.num_cdr_tweets for d in data]),
    #                      np.array([(d.sentiments['Positive'] - d.sentiments['Negative']) / d.num_cdr_tweets * 100
    #                                for d in data]), vlog=True, colorbar=True,
    #                      label=('Num CDR tweets', 'Net sentiment'))
    # figure.show()
    # figure = plot_hist2d(np.array([d.num_cdr_tweets for d in data]),
    #                      np.array([(d.sentiments['Positive'] - d.sentiments['Negative']) / d.num_cdr_tweets * 100
    #                                for d in data]), vlog=True, colorbar=True, xrange=(0, 250),
    #                      label=('Num CDR tweets', 'Net sentiment'))
    # figure.show()
    # figure = plot_hist2d(np.array([d.num_cdr_tweets for d in data]),
    #                      np.array([(d.sentiments['Positive'] - d.sentiments['Negative']) / d.num_cdr_tweets * 100
    #                                for d in data]), vlog=True, colorbar=True, xlog=True,
    #                      label=('Num CDR tweets', 'Net sentiment'))
    # figure.show()
    # figure = plot_hist2d(np.array([d.num_cdr_tweets for d in data]),
    #                      np.array([d.num_tweets for d in data]),
    #                      vlog=True, colorbar=True,
    #                      label=('Num CDR tweets', 'Num tweets'))
    # figure.show()
    # figure = plot_hist2d(np.array([d.num_cdr_tweets for d in data]),
    #                      np.array([d.num_tweets for d in data]),
    #                      vlog=True, colorbar=True, xrange=(0, 200),  # yrange=(0, 2000),
    #                      label=('Num CDR tweets', 'Num tweets'))
    # figure.show()
    # figure = plot_hist2d(np.array([d.num_cdr_tweets for d in data]),
    #                      np.array([d.num_tweets for d in data]),
    #                      vlog=True, colorbar=True, xlog=True, ylog=True,
    #                      label=('Num CDR tweets', 'Num tweets'))
    # figure.show()
    # figure = plot_hist2d(np.array([d.num_cdr_tweets for d in data]),
    #                      np.array([d.num_orig_cdr_tweets for d in data]),
    #                      vlog=True, colorbar=True,
    #                      label=('Num CDR tweets', 'Num orig CDR tweets'))
    # figure.show()
    # figure = plot_hist2d(np.array([d.num_cdr_tweets for d in data]),
    #                      np.array([d.num_orig_cdr_tweets for d in data]),
    #                      vlog=True, colorbar=True, xrange=(0, 300), yrange=(0, 300),
    #                      label=('Num CDR tweets', 'Num orig CDR tweets'))
    # figure.show()
    # figure = plot_hist2d(np.array([d.num_cdr_tweets for d in data]),
    #                      np.array([d.num_orig_cdr_tweets for d in data]),
    #                      vlog=True, colorbar=True,  xlog=True, ylog=True,
    #                      label=('Num CDR tweets', 'Num orig CDR tweets'))
    # figure.show()
    # figure = plot_hist2d(np.array([(d.created_at - datetime.datetime(2006, 3, 21)).days for d in data]),
    #                      np.array([d.time_to_first_cdr.days for d in data]),
    #                      vlog=True, colorbar=True,
    #                      label=('Account creation (days since Mar 2006)', 'Days to first CDR tweet'))
    # figure.show()
    # figure = plot_hist2d(np.array([(d.created_at - datetime.datetime(2006, 3, 21)).days for d in data]),
    #                      np.array([d.time_to_first_cdr.days for d in data]),
    #                      vlog=False, colorbar=True,
    #                      label=('Account creation (days since Mar 2006)', 'Days to first CDR tweet'))
    # figure.show()
    # figure = plot_hist2d(np.array([d.time_cdr_active.days for d in data]),
    #                      np.array([d.num_cdr_tweets for d in data]),
    #                      vlog=True, colorbar=True,
    #                      label=('Days btw. first and last CDR tweet', 'Num CDR tweets'))
    # figure.show()
    # figure = plot_hist2d(np.array([d.time_cdr_active.days for d in data]),
    #                      np.array([d.num_cdr_tweets for d in data]),
    #                      vlog=True, colorbar=True, ylog=True,
    #                      label=('Days btw. first and last CDR tweet', 'Num CDR tweets'))
    # figure.show()
    # figure = plot_hist2d(np.array([d.time_cdr_active.days for d in data]),
    #                      np.array([d.num_cdr_tweets for d in data]),
    #                      vlog=True, colorbar=True,
    #                      yrange=(1, 100), xrange=(1, np.array([d.time_cdr_active.days for d in data]).max()),
    #                      label=('Days btw. first and last CDR tweet', 'Num CDR tweets'))
    # figure.show()
    # figure = plot_hist2d(np.array([d.time_cdr_active.days for d in data]),
    #                      np.array([d.time_to_first_cdr.days for d in data]),
    #                      vlog=True, colorbar=True,
    #                      label=('Days btw. first and last CDR tweet', 'Days to first CDR tweet'))
    # figure.show()


if __name__ == "__main__":
    typer.run(main)
