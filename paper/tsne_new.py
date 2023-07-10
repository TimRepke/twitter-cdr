import datetime
import pickle
import pandas as pd
from matplotlib.gridspec import GridSpec
from matplotlib import ticker

import matplotlib.dates as mdates
from sqlalchemy import text, bindparam, ARRAY, String
import numpy as np
from matplotlib import pyplot as plt
import tikzplotlib
from pathlib import Path
from shared.db import get_data
from shared.vector_index import VectorIndex

stmt = text('''
WITH buckets as (SELECT generate_series('2010-01-01 00:00'::timestamp,
                                        '2022-12-31 00:00'::timestamp,
                                        '3 months') as bucket),
     tweets AS (SELECT ti.item_id,
                       ti.twitter_id,
                       ti.created_at,
                       ti.twitter_author_id,
                       ba_tech.value_int                                                                   as technology,
                       ba_senti.value_int                                                                  as sentiment,
                       (ti."user" -> 'created_at')::text::timestamp                                        as created,
                       extract('day' from date_trunc('day', '2023-01-01'::timestamp -
                                                            (ti."user" -> 'created_at')::text::timestamp)) as days,
                       (ti."user" -> 'tweet_count')::int                                                   as n_tweets
                FROM twitter_item ti
                         LEFT JOIN bot_annotation ba_tech ON (
                            ti.item_id = ba_tech.item_id
                        AND ba_tech.bot_annotation_metadata_id = 'fc73da56-9f51-4d2b-ad35-2a01dbe9b275'
                        AND ba_tech.key = 'tech')
                         LEFT JOIN bot_annotation ba_senti ON (
                            ti.item_id = ba_senti.item_id
                        AND ba_senti.bot_annotation_metadata_id = 'e63da0c9-9bb5-4026-ab5e-7d5845cdc111'
                        AND ba_senti.key = 'senti'
                        AND ba_senti.repeat = 1)
                WHERE ti.project_id = 'c5d36b2e-cbb4-47a8-8370-e5f52bb78bf3'
                  AND ti.created_at >= '2010-01-01'::timestamp
                  AND ti.created_at <= '2023-01-01'::timestamp
                  AND ba_tech.value_int > 1),
     users_pre AS (SELECT twitter_author_id,
                          AVG(days)                  as days,
                          AVG(n_tweets)              as n_tweets,
                          COUNT(DISTINCT twitter_id) as n_cdr_tweets
                   FROM tweets
                   GROUP BY twitter_author_id),
     users AS (SELECT users_pre.*,
                      CASE
                          WHEN n_tweets / days <= 100
                              AND n_cdr_tweets <= 2 THEN 'A'
                          WHEN n_tweets / days <= 100
                              AND n_cdr_tweets > 2
                              AND n_cdr_tweets <= 50 THEN 'B'
                          WHEN n_tweets / days <= 100
                              AND n_cdr_tweets > 50 THEN 'C'
                          ELSE 'EX'
                          END as panel
               FROM users_pre),
     dat AS (SELECT ti.created_at        as created_at,
                    ti.twitter_id        as twitter_id,
                    ti.twitter_author_id as author_id,
                    ti.sentiment         as sentiment,
                    ti.technology        as technology,
                    u.panel              as panel
             FROM tweets ti
                   LEFT OUTER JOIN users u ON ti.twitter_author_id = u.twitter_author_id)
SELECT b.bucket, d.created_at, d.twitter_id, d.author_id, d.sentiment, d.technology, d.panel 
FROM buckets b
    LEFT JOIN dat d ON (
                        d.created_at >= b.bucket
                    AND d.created_at <= (b.bucket + '3 month'::interval));
''')

CACHE = Path('data/tweet_info.pkl')
TSNE_FILE = Path('../data/geo/vectors/vec_2d_tsne_mean_10_all')

print('Loading data...')
tweet_info = get_data(stmt, file=CACHE, params={})
df = pd.DataFrame(tweet_info)
df = df.set_index('twitter_id')

print('Loading vectors...')
vectors = VectorIndex()
vectors.load(TSNE_FILE)

print()
