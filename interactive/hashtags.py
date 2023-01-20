import logging
import re
from pathlib import Path
import datetime

from matplotlib import pyplot as plt
import matplotlib.dates as mdates
from pydantic import BaseModel
from scipy.io.arff._arffread import test_weka
from sqlalchemy import text
from sqlalchemy.orm import Session
from nacsos_data.db import DatabaseEngine

from common.config import settings
from common.db_cache import QueryCache
import streamlit as st
import pandas as pd

from common.queries import queries

logging.basicConfig(format='%(asctime)s [%(levelname)s] %(name)s: %(message)s', level='DEBUG')
logger = logging.getLogger('hashtags')

target_dir = Path(settings.DATA_INTERACTIVE)
target_dir = target_dir.resolve()
logger.info(f'Ensuring target dir exists: {target_dir}')
target_dir.mkdir(exist_ok=True, parents=True)

project_id: str = settings.PROJECT_ID
bot_annotation_tech: str = 'fc73da56-9f51-4d2b-ad35-2a01dbe9b275'
bot_annotation_senti: str = 'e63da0c9-9bb5-4026-ab5e-7d5845cdc111'

db_engine = DatabaseEngine(host=settings.HOST, port=settings.PORT,
                           user=settings.USER, password=settings.PASSWORD,
                           database=settings.DATABASE)
logger.info(f'Connecting to database {db_engine._connection_str}')

query_cache = QueryCache(
    cache_dir=target_dir / 'cache',
    db_engine=db_engine,
    skip_cache=False
)


class TechnologyCounts(BaseModel):
    bucket: datetime.datetime
    Total: int
    counts: dict[str, int]


st.set_page_config(layout='wide', page_title='Explore Tweets on hashtags over time')

with st.sidebar:
    st.subheader('Settings for count plot')
    date_start = st.date_input('Observed timeframe (start)', datetime.datetime(2010, 1, 1, 0, 0, 0))
    date_end = st.date_input('Observed timeframe (end)', datetime.datetime(2022, 12, 31, 23, 59, 59))
    resolution = st.selectbox('Count resolution',
                              options=('1 day', '1 week', '2 weeks', '1 month', '3 months', '1 year'),
                              index=1)

    st.subheader('Settings for hashtag counts')
    window = st.text_input('Time before and after selected centre', '1 week')
    hashtag_limit = st.number_input('Limit to top ... hashtags', 50)

    st.subheader('Settings for hashtag tweet search')
    ht_page_size = st.number_input('Page size', 200)


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
               count(labels.technology)                                                as "Total",
               count(DISTINCT labels.twitter_id) FILTER (WHERE labels.technology = 0)  as "Methane Removal",
               count(DISTINCT labels.twitter_id) FILTER (WHERE labels.technology = 1)  as "CCS",
               count(DISTINCT labels.twitter_id) FILTER (WHERE labels.technology = 2)  as "Ocean Fertilization",
               count(DISTINCT labels.twitter_id) FILTER (WHERE labels.technology = 3)  as "Ocean Alkalinization",
               count(DISTINCT labels.twitter_id) FILTER (WHERE labels.technology = 4)  as "Enhanced Weathering",
               count(DISTINCT labels.twitter_id) FILTER (WHERE labels.technology = 5)  as "Biochar",
               count(DISTINCT labels.twitter_id) FILTER (WHERE labels.technology = 6)  as "Afforestation/Reforestation",
               count(DISTINCT labels.twitter_id) FILTER (WHERE labels.technology = 7)  as "Ecosystem Restoration",
               count(DISTINCT labels.twitter_id) FILTER (WHERE labels.technology = 8)  as "Soil Carbon Sequestration",
               count(DISTINCT labels.twitter_id) FILTER (WHERE labels.technology = 9)  as "BECCS",
               count(DISTINCT labels.twitter_id) FILTER (WHERE labels.technology = 10) as "Blue Carbon",
               count(DISTINCT labels.twitter_id) FILTER (WHERE labels.technology = 11) as "Direct Air Capture",
               count(DISTINCT labels.twitter_id) FILTER (WHERE labels.technology = 12) as "GGR (general)"
        FROM buckets b
                 LEFT JOIN labels ON (
                        labels.created_at >= b.bucket
                    AND labels.created_at <= b.bucket + :resolution ::interval)
        GROUP BY b.bucket
        ORDER BY bucket
        """)
    result = query_cache.query(query, {
        'start_time': datetime.datetime(date_start.year, date_start.month, date_start.day, 0, 0, 0),
        'end_time': datetime.datetime(date_end.year, date_end.month, date_end.day, 23, 59, 59),
        'resolution': resolution,
        'project_id': project_id,
        'bot_tech': bot_annotation_tech,
    })

    def row_to_obj(row) -> TechnologyCounts:
        cp = dict(row)
        del cp['Total']
        del cp['bucket']
        return TechnologyCounts(Total=row['Total'], bucket=row['bucket'], counts=cp)

    return [row_to_obj(r) for r in result]


def fetch_hashtag_counts(window_size: str, center: datetime.date, limit: int):
    query = text("""
        WITH labels as (SELECT DISTINCT ON (twitter_item.twitter_id, ba_tech.value_int) twitter_item.created_at,
                                                                                        twitter_item.twitter_author_id,
                                                                                        twitter_item.twitter_id,
                                                                                        jsonb_array_elements(
                                                                                        CASE
                                                                                            WHEN twitter_item.hashtags = 'null'
                                                                                                THEN '[null]'::jsonb
                                                                                            ELSE
                                                                                                twitter_item.hashtags END) ->>'tag'             
                                                                                                as tag,
                                                                                        ba_tech.value_int                                     as technology
                        FROM twitter_item
                                 LEFT JOIN bot_annotation ba_tech on (
                                    twitter_item.item_id = ba_tech.item_id
                                AND ba_tech.bot_annotation_metadata_id = :ba_tech
                                AND ba_tech.key = 'tech')
                        WHERE twitter_item.project_id = :project_id
                          AND twitter_item.created_at > :center ::timestamp - :window ::interval
                          AND twitter_item.created_at < :center ::timestamp + :window ::interval)
        SELECT tag,
               count(DISTINCT twitter_id) as "Total",
               count(DISTINCT twitter_id) FILTER (WHERE technology = 0)  as "Methane Removal",
               count(DISTINCT twitter_id) FILTER (WHERE technology = 1)  as "CCS",
               count(DISTINCT twitter_id) FILTER (WHERE technology = 2)  as "Ocean Fertilization",
               count(DISTINCT twitter_id) FILTER (WHERE technology = 3)  as "Ocean Alkalinization",
               count(DISTINCT twitter_id) FILTER (WHERE technology = 4)  as "Enhanced Weathering",
               count(DISTINCT twitter_id) FILTER (WHERE technology = 5)  as "Biochar",
               count(DISTINCT twitter_id) FILTER (WHERE technology = 6)  as "Afforestation/Reforestation",
               count(DISTINCT twitter_id) FILTER (WHERE technology = 7)  as "Ecosystem Restoration",
               count(DISTINCT twitter_id) FILTER (WHERE technology = 8)  as "Soil Carbon Sequestration",
               count(DISTINCT twitter_id) FILTER (WHERE technology = 9)  as "BECCS",
               count(DISTINCT twitter_id) FILTER (WHERE technology = 10) as "Blue Carbon",
               count(DISTINCT twitter_id) FILTER (WHERE technology = 11) as "Direct Air Capture",
               count(DISTINCT twitter_id) FILTER (WHERE technology = 12) as "GGR (general)"
        FROM labels
        GROUP BY tag
        ORDER BY "Total" DESC
        LIMIT :limit;
        """)

    with db_engine.session() as session:  # type: Session
        logger.debug(f'Fetching top hashtags around {center}')
        res = session.execute(query, {
            'limit': limit,
            'project_id': project_id,
            'ba_tech': bot_annotation_tech,
            'center': center,
            'window': window_size
        })
        return res.mappings().all()


def fetch_tweets(hashtag: str):
    subquery = ''
    if ht_fix_time:
        subquery = """
          AND ti.created_at > :center ::timestamp - :window ::interval
          AND ti.created_at < :center ::timestamp + :window ::interval
        """

    query = text(f"""
    WITH pre AS (
        SELECT distinct on (ti.twitter_id) ti.created_at,
                                       ti.twitter_id                                              as twitter_id,
                                       --ti.hashtags                                                as hashtags,
                                       i.text                                                     as text,
                                       array_agg(ba.value_int) OVER ( PARTITION BY ti.twitter_id) as technology
        FROM twitter_item ti
                 LEFT JOIN item i on i.item_id = ti.item_id
                 LEFT JOIN bot_annotation ba on ti.item_id = ba.item_id
        WHERE ti.project_id = :project_id
          AND ba.bot_annotation_metadata_id = :ba_tech
          AND ba.key = 'tech'
          AND ti.hashtags @> :ht 
          {subquery})
    SELECT *
    FROM pre
    WHERE technology && :techs ::integer[]
    ORDER BY created_at
    OFFSET :offset LIMIT :limit
    """)

    with db_engine.session() as session:  # type: Session
        logger.debug(f'Fetching tweets for "{hashtag}"')
        res = session.execute(query, {
            'limit': ht_page_size,
            'offset': st.session_state.ht_page * ht_page_size,
            'project_id': project_id,
            'ba_tech': bot_annotation_tech,
            'techs': ht_technologies_int,
            'ht': f'[{{"tag": "{hashtag}"}}]',
            'center': current_center,
            'window': window
        })
        return res.mappings().all()


def str2delta(s: str) -> datetime.timedelta:
    reg = re.compile(r'[^0-9]')
    num = int(reg.sub('', s))
    if 'week' in s:
        return datetime.timedelta(days=7 * num)
    if 'month' in s:
        return datetime.timedelta(days=30 * num)
    if 'day' in s:
        return datetime.timedelta(days=num)
    if 'year' in s:
        return datetime.timedelta(days=365 * num)
    return datetime.timedelta(days=7)


def plot_temporal_count(hist: pd.DataFrame, techs: list[str]):
    fig, ax = plt.subplots(figsize=(12, 4), dpi=200)

    for tech in techs:
        ax.plot(hist.index, hist[tech], label=tech)

    ax.set_xlim(hist.index[0], hist.index[-1])

    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=6))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax.xaxis.set_minor_locator(mdates.MonthLocator(interval=1))

    locator = mdates.YearLocator()
    locator.set_axis(ax.xaxis)
    for yr in locator():
        ax.axvline(x=yr, ls=':', color='lightgrey', linewidth=1)

    ax.grid(which='major', axis='y', ls=':', color='lightgrey', linewidth=1)

    delta = str2delta(window)
    ax.axvspan(current_center - delta,
               current_center + delta,
               color='black', alpha=0.2, lw=0)
    ax.axvline(x=current_center, ls='-', color='black', linewidth=1)
    ax.legend()
    fig.autofmt_xdate()
    fig.tight_layout()
    return fig


histogram_data = fetch_tech_counts()
histogram = pd.DataFrame({
    'Bucket': [d.bucket for d in histogram_data],
    'Total': [d.Total for d in histogram_data],
    **{
        tech: [d.counts[tech] for d in histogram_data]
        for tech in queries.keys()
    }
})
histogram.set_index('Bucket', inplace=True)
current_center = st.slider('Investigation centre',
                           min_value=datetime.datetime(date_start.year, date_start.month, date_start.day, 0, 0, 0),
                           max_value=datetime.datetime(date_end.year, date_end.month, date_end.day, 23, 59, 59),
                           value=datetime.datetime(date_start.year, date_start.month, date_start.day, 12, 0, 0),
                           step=datetime.timedelta(days=1))

technologies = list(queries.keys()) + ['Total']
plot_technologies = st.multiselect('Technologies in plot', technologies, ['Total'])
st.markdown(f'First day in aggregation: **{histogram.index[0]}**; Last day in aggregation: **{histogram.index[-1]}**; ')

figure = plot_temporal_count(histogram, plot_technologies)
st.pyplot(figure, clear_figure=True)

st.subheader('Hashtag counts')
st.markdown(f'Showing hashtag counts **{window}** around **{current_center}** '
            f'limited to the top **{hashtag_limit}** hashtags')
hashtags = pd.DataFrame(fetch_hashtag_counts(window_size=window, center=current_center, limit=hashtag_limit))
hashtags.set_index('tag', inplace=True)
t = hashtags.pop('Total')
hashtags.insert(0, 'Total', t)
st.dataframe(hashtags, use_container_width=True)

technology2num = {tn: ti for ti, tn in enumerate(technologies)}
num2technology = {ti: tn for ti, tn in enumerate(technologies)}

if 'ht_page' not in st.session_state:
    st.session_state['ht_page'] = 0


def turn(d: int):
    st.session_state.ht_page += d
    logger.info(f'Turning page by {d} to {st.session_state.ht_page}')
    if st.session_state.ht_page < 0:
        st.session_state.ht_page = 0

st.subheader('Tweets')
cols_ht_set = st.columns([4,1])
ht_hashtags = cols_ht_set[0].multiselect('Hashtags to search for', hashtags.index.values, hashtags.index.values[1])
ht_technologies = cols_ht_set[0].multiselect('Technologies for hashtag search', technologies, technologies)
ht_technologies_int = [technology2num[t] for t in ht_technologies]
ht_fix_time = cols_ht_set[1].checkbox('Limit to time above', True)


cols_paging = st.columns([1, 1, 5, 1, 1])
cols_paging[0].button('Prev', on_click=lambda: turn(-1))
cols_paging[1].button('First', on_click=lambda: turn(-8000))
cols_paging[2].markdown(f'Page **{st.session_state.ht_page}**')
cols_paging[4].button('Next', on_click=lambda: turn(1))

try:
    tweets = fetch_tweets('carbon')
    ht_tweets = pd.DataFrame(tweets)
    ht_tweets['technologies'] = [[num2technology[i] for i in set(row)] for row in ht_tweets.pop('technology')]
    ht_tweets.set_index(ht_tweets.pop('created_at'), inplace=True)
    st.dataframe(ht_tweets, use_container_width=True)
except:
    st.session_state.ht_page = 0
    st.text('No tweets found.')