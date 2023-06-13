import pickle
from pathlib import Path

import pandas as pd
from sqlalchemy import text
import numpy as np
from shared.db import run_query
from shared.queries import queries

START = '2006-01-01 00:00'
END = '2022-12-31 23:59'
BUCKET_SIZE = '1 year'


def get_data(cache: Path | None = None):
    if cache and cache.exists():
        with open(cache, 'rb') as fin:
            return pickle.load(fin)

    else:
        # WARNING: Sum won't properly add up, because tweets may have more than one technology!
        stmt = text('''
            WITH buckets as (SELECT generate_series(:start_time ::timestamp,
                                                    :end_time ::timestamp,
                                                    :bucket_size) as bucket),
                 tweets AS (SELECT ti.item_id,
                                ti.twitter_id,
                                ti.created_at,
                                ti.twitter_author_id,
                                ba_tech.value_int                                                                   as technology,
                                ba_senti.value_int                                                                  as sentiment,
                                (ti."user" -> 'created_at')::text::timestamp                                        as created,
                                extract('day' from date_trunc('day', :end_time ::timestamp - (ti."user" -> 'created_at')::text::timestamp)) as days,
                                (ti."user" -> 'tweet_count')::int                                                   as n_tweets
                         FROM twitter_item ti
                                  LEFT JOIN bot_annotation ba_tech ON (
                                         ti.item_id = ba_tech.item_id
                                     AND ba_tech.bot_annotation_metadata_id = :ba_tech
                                     AND ba_tech.key = 'tech')
                                 LEFT JOIN bot_annotation ba_senti ON (
                                       ti.item_id = ba_senti.item_id
                                   AND ba_senti.bot_annotation_metadata_id = :ba_senti
                                   AND ba_senti.key = 'senti'
                                   AND ba_senti.repeat = 1)
                         WHERE ti.project_id = :project_id
                           AND ti.created_at >= :start_time ::timestamp
                           AND ti.created_at <= :end_time ::timestamp),
                 users_pre AS (SELECT twitter_author_id,
                                      AVG(days)                                                as days,
                                      AVG(n_tweets)                                            as n_tweets,
                                      COUNT(DISTINCT twitter_id)                               as n_cdr_tweets,
                                      COUNT(DISTINCT twitter_id) FILTER (WHERE technology > 1) as n_cdr_tweets_noccs
                           FROM tweets
                           GROUP BY twitter_author_id),
                 users AS (SELECT users_pre.*,
                                  CASE
                                      WHEN n_tweets / days <= 100
                                          AND n_cdr_tweets > 3 THEN 'A'
                                      WHEN n_tweets / days <= 100
                                          AND n_cdr_tweets >= 2
                                          AND n_cdr_tweets <= 3 THEN 'B'
                                      WHEN n_tweets / days <= 100
                                          AND n_cdr_tweets = 1 THEN 'C'
                                      ELSE 'EX'
                                      END as panel
                           FROM users_pre)
            SELECT date_part('year', b.bucket)            as bucket,
                   u.panel                                as panel,
                   ti.sentiment                           as sentiment,
                   ti.technology                          as technology,
                   count(DISTINCT ti.twitter_id)          as num_tweets,
                   count(DISTINCT ti.twitter_author_id)   as num_users
            FROM buckets b
                     LEFT OUTER JOIN tweets ti ON (
                            ti.created_at >= b.bucket 
                        AND ti.created_at < (b.bucket + :bucket_size ::interval))
                     LEFT OUTER JOIN users u ON ti.twitter_author_id = u.twitter_author_id
            GROUP BY b.bucket, u.panel, ti.sentiment, ti.technology;
            ''')

        print('Running query')
        result = run_query(stmt, {
            'project_id': 'c5d36b2e-cbb4-47a8-8370-e5f52bb78bf3',
            'ba_tech': 'fc73da56-9f51-4d2b-ad35-2a01dbe9b275',
            'ba_senti': 'e63da0c9-9bb5-4026-ab5e-7d5845cdc111',
            'bucket_size': BUCKET_SIZE,
            'start_time': START,
            'end_time': END
        })

        with open(cache, 'wb') as fout:
            pickle.dump(result, fout)

        return result


dat = get_data(cache=Path('data/yearly_tweets.pkl'))

df = pd.DataFrame(dat)
print(df)


def tech_count_table(values):
    tab = pd.pivot_table(df,
                         values=values,
                         index=['technology'],
                         columns=['panel'],
                         aggfunc=np.sum,
                         fill_value=0,
                         margins=True)
    tab.reset_index(inplace=True)

    tab = pd.concat([tab,
                     pd.DataFrame([{
                         'technology': 'Total',
                         'A': tab['A'][2:13].sum(),
                         'B': tab['B'][2:13].sum(),
                         'C': tab['C'][2:13].sum(),
                         'EX': tab['EX'][2:13].sum(),
                         'All': tab['All'][2:13].sum(),
                     }])], ignore_index=True)

    TECHS = list(queries.keys())
    tab['technology'] = [TECHS[int(t)] if type(t) == float else t for t in tab['technology']]
    tab.set_index('technology', inplace=True)
    print(tab.style
          .format(subset="A", precision=1, thousands=",")
          .format(subset="B", precision=1, thousands=",")
          .format(subset="C", precision=1, thousands=",")
          .format(subset="EX", precision=1, thousands=",")
          .format(subset="All", precision=1, thousands=",")
          .to_latex(multirow_align="t", multicol_align="r", hrules=True))
    return tab


t1 = tech_count_table('num_tweets')
t2 = tech_count_table('num_users')

print(t1 / t2)
