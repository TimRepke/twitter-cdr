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


class SentimentCounts(BaseModel):
    bucket: datetime
    num_tweets: int
    num_negative: int
    num_neutral: int
    num_positive: int


class TechnologyCounts(BaseModel):
    bucket: datetime
    num_tweets: int
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
        target_dir = Path(settings.DATA_FIGURES) / 'histogram_counts'
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

    def fetch_tech_counts():
        query = text("""
            WITH buckets as (SELECT generate_series(:start_time ::timestamp,
                                                    :end_time ::timestamp,
                                                    :resolution) as bucket),
                 labels as (SELECT DISTINCT ON (twitter_item.twitter_id, ba.value_int)
                                   twitter_item.created_at,
                                   to_char(twitter_item.created_at, 'YYYY-MM-DD') as day,
                                   twitter_item.twitter_id                        as twitter_id,
                                   ba.value_int                                   as technology
                            FROM twitter_item
                                     LEFT JOIN bot_annotation ba on twitter_item.item_id = ba.item_id
                            WHERE twitter_item.project_id = :project_id
                              AND ba.bot_annotation_metadata_id = :bot_tech
                              AND ba.key = 'tech')
            SELECT b.bucket                                                                as bucket,
                   count(labels.technology)                                                as num_tweets,
                   count(DISTINCT labels.twitter_id) FILTER (WHERE labels.technology = 0)  as "Methane removal",
                   count(DISTINCT labels.twitter_id) FILTER (WHERE labels.technology = 1)  as "CCS",
                   count(DISTINCT labels.twitter_id) FILTER (WHERE labels.technology = 2)  as "Ocean fertilization",
                   count(DISTINCT labels.twitter_id) FILTER (WHERE labels.technology = 3)  as "Ocean alkalinization",
                   count(DISTINCT labels.twitter_id) FILTER (WHERE labels.technology = 4)  as "Enhanced weathering",
                   count(DISTINCT labels.twitter_id) FILTER (WHERE labels.technology = 5)  as "Biochar",
                   count(DISTINCT labels.twitter_id) FILTER (WHERE labels.technology = 6)  as "Afforestation/reforestation",
                   count(DISTINCT labels.twitter_id) FILTER (WHERE labels.technology = 7)  as "Ecosystem restoration",
                   count(DISTINCT labels.twitter_id) FILTER (WHERE labels.technology = 8)  as "Soil carbon sequestration",
                   count(DISTINCT labels.twitter_id) FILTER (WHERE labels.technology = 9)  as "BECCS",
                   count(DISTINCT labels.twitter_id) FILTER (WHERE labels.technology = 10) as "Blue carbon",
                   count(DISTINCT labels.twitter_id) FILTER (WHERE labels.technology = 11) as "Direct air capture",
                   count(DISTINCT labels.twitter_id) FILTER (WHERE labels.technology = 12) as "GGR (general)"
            FROM buckets b
                     LEFT JOIN labels ON (
                            labels.created_at >= b.bucket
                        AND labels.created_at < b.bucket + :resolution ::interval)
            GROUP BY b.bucket
            ORDER BY bucket;
            """)
        result = query_cache.query(query, {
            'start_time': start_time,
            'end_time': end_time,
            'resolution': interval,
            'project_id': project_id,
            'bot_tech': bot_annotation_tech,
        })

        def row_to_obj(row) -> TechnologyCounts:
            cp = dict(row)
            del cp['num_tweets']
            del cp['bucket']
            return TechnologyCounts(num_tweets=row['num_tweets'], bucket=row['bucket'], counts=cp)

        return [row_to_obj(r) for r in result]

    def fetch_tech_sentiments(technology: int) -> list[SentimentCounts]:
        query = text("""
            WITH buckets as (SELECT generate_series(:start_time ::timestamp,
                                                    :end_time ::timestamp,
                                                    :resolution) as bucket),
                 labels as (SELECT DISTINCT ON (twitter_item.twitter_id, ba_tech.value_int) twitter_item.created_at,
                                                                                            to_char(twitter_item.created_at, 'YYYY-MM-DD') as day,
                                                                                            ba_sent.value_int                              as sentiment,
                                                                                            ba_tech.value_int                              as technology
                            FROM twitter_item
                                     LEFT OUTER JOIN bot_annotation ba_tech on (
                                        twitter_item.item_id = ba_tech.item_id
                                    AND ba_tech.bot_annotation_metadata_id = :bot_tech
                                    AND ba_tech.key = 'tech')
                                     LEFT JOIN bot_annotation ba_sent on (
                                        ba_tech.item_id = ba_sent.item_id
                                    AND ba_sent.bot_annotation_metadata_id = :bot_senti
                                    AND ba_sent.repeat = 1
                                    AND ba_sent.key = 'senti')
                            WHERE twitter_item.project_id = :project_id)
            SELECT b.bucket                                     as bucket,
                   count(labels.sentiment)                      as num_tweets,
                   count(1) FILTER (WHERE labels.sentiment = 0) as num_negative,
                   count(1) FILTER (WHERE labels.sentiment = 1) as num_neutral,
                   count(1) FILTER (WHERE labels.sentiment = 2) as num_positive
            FROM buckets b
                 LEFT JOIN labels ON (
                    labels.created_at >= b.bucket
                AND labels.created_at < b.bucket + :resolution ::interval
                AND labels.technology = :technology)
            GROUP BY b.bucket
            ORDER BY bucket;
        """)
        result = query_cache.query(query, {
            'start_time': start_time,
            'end_time': end_time,
            'resolution': interval,
            'project_id': project_id,
            'bot_senti': bot_annotation_senti,
            'bot_tech': bot_annotation_tech,
            'technology': technology
        })
        return [SentimentCounts.parse_obj(r) for r in result]

    def plot_temporal_count(counts: list[TechnologyCounts]):
        dates = [d.bucket for d in counts]
        fig, ax = plt.subplots()
        fig.suptitle(
            f'Tweet counts\n aggregation interval "{interval}", smoothed across {smoothing_windowsize} intervals')
        for t in counts[0].counts.keys():
            ax.plot(dates, smooth([[d.counts[t] for d in counts]])[0], label=t)
        ax.set_xlim(dates[0], dates[-1])
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        fig.autofmt_xdate()
        box = ax.get_position()
        ax.set_position([box.x0, box.y0, box.width * 0.8, box.height])
        ax_handles, ax_labels = ax.get_legend_handles_labels()
        ax.legend(ax_handles[::-1], ax_labels[::-1], loc='center left', bbox_to_anchor=(1, 0.5))
        fig.tight_layout()
        return fig

    def plot_temporal_count_stacked(counts: list[TechnologyCounts], relative: bool):
        dates = [d.bucket for d in counts]
        technologies = list(counts[0].counts.keys())
        n_tweets = np.array([r.num_tweets for r in counts], dtype=float)
        stacked = np.array([[d.counts[t] for d in counts] for t in technologies], dtype=float)
        stacked_smooth = smooth(stacked, with_pad=True)
        shares = 100 * np.divide(stacked, n_tweets, out=np.zeros_like(stacked), where=n_tweets != 0)
        shares_smooth = smooth(shares, with_pad=True)

        fig, ax = plt.subplots()
        fig.suptitle(
            f'Tweet counts per technology\naggregation interval: "{interval}", smoothed across {smoothing_windowsize} intervals')
        if relative:
            ax.stackplot(dates, shares_smooth, labels=technologies)
            ax.set_ylim(0, 100)
        else:
            ax.stackplot(dates, stacked_smooth, labels=technologies)
        ax.set_xlim(dates[0], dates[-1])
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        fig.autofmt_xdate()

        box = ax.get_position()
        ax.set_position([box.x0, box.y0, box.width * 0.8, box.height])
        ax_handles, ax_labels = ax.get_legend_handles_labels()
        ax.legend(ax_handles[::-1], ax_labels[::-1], loc='center left', bbox_to_anchor=(1, 0.5))

        fig.tight_layout()

        return fig

    def plot_sentiments_temp_ax(ax: plt.Axes,
                                counts: list[SentimentCounts],
                                relative: bool) -> None:
        colors = [['darkred', 'white', 'g'], ['mediumslateblue', 'white', 'lightseagreen'], ['darkred', 'y', 'g']]

        dates = [r.bucket for r in counts]
        n_tweets = np.array([r.num_tweets for r in counts], dtype=float)
        n_sentis = np.array([[r.num_negative, r.num_neutral, r.num_positive] for r in counts], dtype=float).T

        # n_tweets_smooth = smooth([n_tweets], with_pad=True)[0]
        n_sentis_smooth = smooth(n_sentis, with_pad=True)

        shares = 100 * np.divide(n_sentis, n_tweets, out=np.zeros_like(n_sentis), where=n_tweets != 0)
        shares[1] = 100 - shares[0] - shares[2]
        shares_smooth = smooth(shares, with_pad=True)
        logger.debug(f' > Shape of sentiments: {n_sentis.shape} | {shares.shape} | {shares_smooth.shape}')

        if relative:
            ax.stackplot(dates, shares_smooth, colors=colors[0])
            ax.set_ylim(0, 100)

            # set up arrays for convenience
            x = np.arange(len(dates))
            y_pos = shares[2]
            y_neg = shares[0]

            # fit trend for positive sentiments
            fit_pos = np.polyfit(x, y_pos, 1)
            fit_fn_pos = np.poly1d(fit_pos)
            trend_pos = fit_fn_pos[1]
            trendline_pos = fit_fn_pos(x)

            # fit trend for negative sentiments
            fit_neg = np.polyfit(x, y_neg, 1)
            fit_fn_neg = np.poly1d(fit_neg)
            trend_neg = fit_fn_neg[1]
            trendline_neg = fit_fn_neg(x)

            # fit net trend
            fit_net = np.polyfit(x, y_pos - y_neg, 1)
            fit_fn_net = np.poly1d(fit_net)
            trend_net = fit_fn_net[1]

            trends_text = f'negative: {trend_neg:2.4f}%/day\n' \
                          f'positive: {trend_pos:2.4f}%/day\n' \
                          f'net: {trend_net:2.4f}%/day\n'

            # draw trendlines
            ax.plot(dates, trendline_neg, color='grey')
            ax.plot(dates, 100 - trendline_pos, color='r')
            ax.text(datetime(2021, 12, 1), 50, trends_text, va='center', ha='right')
        else:
            ax.stackplot(dates, n_sentis_smooth, colors=colors[2])

        ax.set_xlim(dates[0], dates[-1])

    def plot_sentiments_temp(counts: list[SentimentCounts],
                             technology: str,
                             relative: bool) -> plt.Figure:
        fig, ax = plt.subplots()
        plot_sentiments_temp_ax(ax, counts, relative)
        fig.suptitle(
            f'Sentiments for {technology}\naggregation interval: "{interval}", smoothed across {smoothing_windowsize} intervals')
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        fig.autofmt_xdate()
        fig.tight_layout()
        return fig

    def plot_sentiments_temp_all(counts: dict[str, list[SentimentCounts]],
                                 relative: bool) -> plt.Figure:
        n_cols = 2
        n_rows = int(np.ceil(len(queries) / n_cols))

        logger.info(f'Creating aggregate figure with {n_rows} rows and {n_cols} columns.')

        fig, axes = plt.subplots(n_rows, n_cols, figsize=(4 * n_cols, 3 * n_rows))
        fig.suptitle(f'Sentiments per technology\n'
                     f'aggregation interval: "{interval}", smoothed across {smoothing_windowsize} intervals\n')

        for i, (technology, tech_counts) in enumerate(counts.items()):
            row = i // n_cols
            col = i % n_cols
            logger.debug(f' -> plotting {technology} to row {row}; column {col}')
            ax = axes[row][col]
            ax.set_title(technology)
            plot_sentiments_temp_ax(ax, tech_counts, relative)
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))

        fig.autofmt_xdate()
        fig.tight_layout()
        return fig

    logger.info('Fetching technology counts...')
    data = fetch_tech_counts()

    logger.debug(' > Creating line plot for tweet counts per technology...')
    figure = plot_temporal_count(data)
    show_save(figure, target_dir / 'counts_temporal' / f'tweets_line_{resolution.value}_all')
    figure = plot_temporal_count_stacked(data, relative=False)
    show_save(figure, target_dir / 'counts_temporal' / f'tweets_stacked_abs_{resolution.value}_all')
    figure = plot_temporal_count_stacked(data, relative=True)
    show_save(figure, target_dir / 'counts_temporal' / f'tweets_stacked_rel_{resolution.value}_all')

    data_acc = {}
    for tech_i, (technology_name, sub_queries) in enumerate(queries.items()):
        logger.info(f'Fetching data of sentiments for technology "{technology_name}" ({tech_i})')

        data = fetch_tech_sentiments(tech_i)
        data_acc[technology_name] = data
        figure = plot_sentiments_temp(data, technology=technology_name, relative=True)
        show_save(figure, target_dir / 'sentiments_temporal' / f'tempo_{resolution.value}_rel_tech_{tech_i}')
        figure = plot_sentiments_temp(data, technology=technology_name, relative=False)
        show_save(figure, target_dir / 'sentiments_temporal' / f'tempo_{resolution.value}_abs_tech_{tech_i}')

    figure = plot_sentiments_temp_all(data_acc, relative=True)
    show_save(figure, target_dir / 'sentiments_temporal' / f'tempo_{resolution.value}_rel_tech_all')
    figure = plot_sentiments_temp_all(data_acc, relative=False)
    show_save(figure, target_dir / 'sentiments_temporal' / f'tempo_{resolution.value}_abs_tech_all')


if __name__ == "__main__":
    typer.run(main)
