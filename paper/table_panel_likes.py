import pickle
from pathlib import Path

import pandas as pd
from sqlalchemy import text
import numpy as np
from shared.db import run_query

START = '2006-01-01 00:00'
END = '2022-12-31 23:59'
BUCKET_SIZE = '1 year'


def get_data(cache: Path | None = None):
    if cache and cache.exists():
        with open(cache, 'rb') as fin:
            return pickle.load(fin)

    else:
        stmt = text('''
            WITH tweets AS (SELECT ti.item_id,
                                   ti.twitter_id,
                                   ti.created_at,
                                   ti.twitter_author_id,
                                   ti.like_count,
                                   ti.reply_count,
                                   ti.retweet_count,
                                   ba_tech.value_int                                                                   as technology,
                                   (ti."user" -> 'created_at')::text::timestamp                                        as created,
                                   extract('day' from date_trunc('day', :end_time ::timestamp -
                                                                        (ti."user" -> 'created_at')::text::timestamp)) as days,
                                   (ti."user" -> 'tweet_count')::int                                                   as n_tweets
                            FROM twitter_item ti
                                     LEFT JOIN bot_annotation ba_tech ON (
                                        ti.item_id = ba_tech.item_id
                                    AND ba_tech.bot_annotation_metadata_id = :ba_tech
                                    AND ba_tech.key = 'tech')
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
                                          AND n_cdr_tweets <= 2 THEN 'A'
                                      WHEN n_tweets / days <= 100
                                          AND n_cdr_tweets > 2
                                          AND n_cdr_tweets <= 50 THEN 'B'
                                      WHEN n_tweets / days <= 100
                                          AND n_cdr_tweets > 50 THEN 'C'
                                      ELSE 'EX'
                                      END as panel
                           FROM users_pre)
            SELECT ti.technology as technology,
                   panel,
                   COUNT(DISTINCT twitter_id) as n_tweets,
                   AVG(like_count) as avg_likes,
                   AVG(retweet_count) as avg_rt,
                   AVG(reply_count) as avg_reply
            FROM tweets ti
                     LEFT OUTER JOIN users u ON ti.twitter_author_id = u.twitter_author_id
            GROUP BY ti.technology, panel
            UNION
            SELECT ti.technology as technology,
                   'All'         as panel,
                   COUNT(DISTINCT twitter_id) as n_tweets,
                   AVG(like_count) as avg_likes,
                   AVG(retweet_count) as avg_rt,
                   AVG(reply_count) as avg_reply
            FROM tweets ti
                     LEFT OUTER JOIN users u ON ti.twitter_author_id = u.twitter_author_id
            GROUP BY ti.technology
            UNION
            SELECT 100 as technology,
                   panel,
                   COUNT(DISTINCT twitter_id) as n_tweets,
                   AVG(like_count) as avg_likes,
                   AVG(retweet_count) as avg_rt,
                   AVG(reply_count) as avg_reply
            FROM tweets ti
                     LEFT OUTER JOIN users u ON ti.twitter_author_id = u.twitter_author_id
            WHERE technology > 1
            GROUP BY panel
            UNION
            SELECT 100   as technology,
                   'All' as panel,
                   COUNT(DISTINCT twitter_id) as n_tweets,
                   AVG(like_count) as avg_likes,
                   AVG(retweet_count) as avg_rt,
                   AVG(reply_count) as avg_reply
            FROM tweets ti
                     LEFT OUTER JOIN users u ON ti.twitter_author_id = u.twitter_author_id
            WHERE technology > 1
            UNION
            SELECT 200 as technology,
                   panel,
                   COUNT(DISTINCT twitter_id) as n_tweets,
                   AVG(like_count) as avg_likes,
                   AVG(retweet_count) as avg_rt,
                   AVG(reply_count) as avg_reply
            FROM tweets ti
                     LEFT OUTER JOIN users u ON ti.twitter_author_id = u.twitter_author_id
            GROUP BY panel
            UNION
            SELECT 200   as technology,
                   'All' as panel,
                   COUNT(DISTINCT twitter_id) as n_tweets,
                   AVG(like_count) as avg_likes,
                   AVG(retweet_count) as avg_rt,
                   AVG(reply_count) as avg_reply
            FROM tweets ti
                     LEFT OUTER JOIN users u ON ti.twitter_author_id = u.twitter_author_id
            ORDER BY 1, 2;
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


dat = get_data(cache=Path('data/group_likes.pkl'))

df = pd.DataFrame(dat)
print(df)
TECHS = {
    0: 'Methane Removal',
    1: 'CCS',
    2: 'Ocean Fertilization',
    3: 'Ocean Alkalinization',
    4: 'Enhanced Weathering',
    5: 'Biochar',
    6: 'Afforestation/Reforestation',
    7: 'Ecosystem Restoration',
    8: 'Soil Carbon Sequestration',
    9: 'BECCS',
    10: 'Blue Carbon',
    11: 'Direct Air Capture',
    12: 'GGR (general)',
    100: 'Total',
    200: 'Total (incl. CCS\&MR)'
}

ORDER = [12, 11, 4, 3, 2, 10, 8, 7, 6, 5, 9, 100, 1, 0, 200]


def print_table(tab, precision=1, incl_ex=True):
    print('\\begin{tabular}{lccccc}')
    print('\\toprule')
    print('Technology & Infrequent & Moderate & Frequent & EX & All \\\\')
    print('\\midrule')
    for ri, row in tab.iterrows():

        print(f'{ri} '
              f'& {row["A_likes"]:.{precision}f} / {row["A_reply"]:.{precision}f} / {row["A_rt"]:.{precision}f} '
              f'& {row["B_likes"]:.{precision}f} / {row["B_reply"]:.{precision}f} / {row["B_rt"]:.{precision}f} '
              f'& {row["C_likes"]:.{precision}f} / {row["C_reply"]:.{precision}f} / {row["C_rt"]:.{precision}f} ',
              end='')
        if incl_ex:
            print(
                f'& {row["EX_likes"]:.{precision}f} / {row["EX_reply"]:.{precision}f} / {row["EX_rt"]:.{precision}f} ',
                end='')
        print(
            f'& {row["All_likes"]:.{precision}f} / {row["All_reply"]:.{precision}f} / {row["All_rt"]:.{precision}f}\\\\')

        if ri in {'Total', 'Methane Removal', 'BECCS'}:
            print('\\midrule')

    print('\\bottomrule\n'
          '\\end{tabular}')


def print_table_mc(tab, precision=1, groups=None, metrics=None):
    print('\\begin{tabular}{lccccc}')
    print('\\toprule')
    print('Technology '
          '& \\multicolumn{3}{c}{Infrequent} '
          '& \\multicolumn{3}{c}{Moderate} '
          '& \\multicolumn{3}{c}{Frequent} '
          '& \\multicolumn{3}{c}{EX} '
          '& \\multicolumn{3}{c}{All} \\\\')
    print('\\cmidrule(lr){2-4} \\cmidrule(lr){5-7} \\cmidrule(lr){8-10} \\cmidrule(lr){11-13} \\cmidrule(lr){14-16}')

    if groups is None:
        groups = ['A', 'B', 'C', 'EX', 'All']
    if metrics is None:
        metrics = ['likes', 'reply', 'rt']

    for ri, row in tab.iterrows():
        print(ri, end=' ')
        for g in groups:
            for m in metrics:
                v = row[f'{g}_{m}']
                print(f'& {v:.{precision}f}', end=' ')
        print('\\\\')

        if ri in {'Total', 'Methane Removal', 'BECCS'}:
            print('\\midrule')

    print('\\bottomrule\n'
          '\\end{tabular}')


def tech_count_table():
    data = {}
    for d in dat:
        key = int(d['technology'])
        if key not in data:
            data[key] = {'Technology': TECHS[key]}
        data[key][f"{d['panel']}_likes"] = d['avg_likes']
        data[key][f"{d['panel']}_reply"] = d['avg_reply']
        data[key][f"{d['panel']}_rt"] = d['avg_rt']

    tab = pd.DataFrame([data[di] for di in ORDER])
    tab.set_index('Technology', inplace=True)
    print(tab)
    print_table(tab)
    print_table_mc(tab)
    return tab


t1 = tech_count_table()
