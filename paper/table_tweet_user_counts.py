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
        stmt = text('''
            WITH tweets AS (SELECT ti.item_id,
                                   ti.twitter_id,
                                   ti.created_at,
                                   ti.twitter_author_id,
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
            SELECT ti.technology                                                        as technology,
                   count(DISTINCT ti.twitter_id) filter ( where u.panel = 'A' )         as tweets_a,
                   count(DISTINCT ti.twitter_id) filter ( where u.panel = 'B' )         as tweets_b,
                   count(DISTINCT ti.twitter_id) filter ( where u.panel = 'C' )         as tweets_c,
                   count(DISTINCT ti.twitter_id) filter ( where u.panel = 'EX' )        as tweets_ex,
                   count(DISTINCT ti.twitter_id)                                        as tweets_all,
                   count(DISTINCT ti.twitter_author_id) filter ( where u.panel = 'A' )  as users_a,
                   count(DISTINCT ti.twitter_author_id) filter ( where u.panel = 'B' )  as users_b,
                   count(DISTINCT ti.twitter_author_id) filter ( where u.panel = 'C' )  as users_c,
                   count(DISTINCT ti.twitter_author_id) filter ( where u.panel = 'EX' ) as users_ex,
                   count(DISTINCT ti.twitter_author_id)                                 as users_all
            FROM tweets ti
                     LEFT OUTER JOIN users u ON ti.twitter_author_id = u.twitter_author_id
            GROUP BY ti.technology
            UNION
            SELECT 100                                                                  as technology,
                   count(DISTINCT ti.twitter_id) filter ( where u.panel = 'A' )         as tweets_a,
                   count(DISTINCT ti.twitter_id) filter ( where u.panel = 'B' )         as tweets_b,
                   count(DISTINCT ti.twitter_id) filter ( where u.panel = 'C' )         as tweets_c,
                   count(DISTINCT ti.twitter_id) filter ( where u.panel = 'EX' )        as tweets_ex,
                   count(DISTINCT ti.twitter_id)                                        as tweets_all,
                   count(DISTINCT ti.twitter_author_id) filter ( where u.panel = 'A' )  as users_a,
                   count(DISTINCT ti.twitter_author_id) filter ( where u.panel = 'B' )  as users_b,
                   count(DISTINCT ti.twitter_author_id) filter ( where u.panel = 'C' )  as users_c,
                   count(DISTINCT ti.twitter_author_id) filter ( where u.panel = 'EX' ) as users_ex,
                   count(DISTINCT ti.twitter_author_id)                                 as users_all
            FROM tweets ti
                     LEFT OUTER JOIN users u ON ti.twitter_author_id = u.twitter_author_id
            WHERE ti.technology > 1
            UNION
            SELECT 200                                                                  as technology,
                   count(DISTINCT ti.twitter_id) filter ( where u.panel = 'A' )         as tweets_a,
                   count(DISTINCT ti.twitter_id) filter ( where u.panel = 'B' )         as tweets_b,
                   count(DISTINCT ti.twitter_id) filter ( where u.panel = 'C' )         as tweets_c,
                   count(DISTINCT ti.twitter_id) filter ( where u.panel = 'EX' )        as tweets_ex,
                   count(DISTINCT ti.twitter_id)                                        as tweets_all,
                   count(DISTINCT ti.twitter_author_id) filter ( where u.panel = 'A' )  as users_a,
                   count(DISTINCT ti.twitter_author_id) filter ( where u.panel = 'B' )  as users_b,
                   count(DISTINCT ti.twitter_author_id) filter ( where u.panel = 'C' )  as users_c,
                   count(DISTINCT ti.twitter_author_id) filter ( where u.panel = 'EX' ) as users_ex,
                   count(DISTINCT ti.twitter_author_id)                                 as users_all
            FROM tweets ti
                     LEFT OUTER JOIN users u ON ti.twitter_author_id = u.twitter_author_id
            ORDER BY 1;
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


dat = get_data(cache=Path('data/user_tweet_counts.pkl'))

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


def print_table(tab, incl_perc=True, precision=0):
    print('\\begin{tabular}{lrrrrr}\n'
          '\\toprule\n'
          'Technology & A & B & C & EX & All \\\\\n'
          '\\midrule')
    for ri, row in tab.iterrows():
        if incl_perc:
            rel = row / row['All']
            txt = (f'{ri} '
                   f'& {row["A"]:,} {{\\footnotesize({rel["A"]:.0%})}} '
                   f'& {row["B"]:,} {{\\footnotesize({rel["B"]:.0%})}} '
                   f'& {row["C"]:,} {{\\footnotesize({rel["C"]:.0%})}} '
                   f'& {row["EX"]:,} '
                   f'& {row["All"]:,} \\\\')
            print(txt.replace('%', '\%'))
        else:
            print(f'{ri} '
                  f'& {row["A"]:.{precision}f} '
                  f'& {row["B"]:.{precision}f} '
                  f'& {row["C"]:.{precision}f} '
                  f'& {row["EX"]:.{precision}f} '
                  f'& {row["All"]:.{precision}f} \\\\')

        if ri in {'Total', 'Methane Removal', 'BECCS'}:
            print('\\midrule')

    print('\\bottomrule\n'
          '\\end{tabular}')


def tech_count_table(prefix):
    data = {
        int(d['technology']): {
            'Technology': TECHS[int(d['technology'])],
            'A': d[f'{prefix}_a'],
            'B': d[f'{prefix}_b'],
            'C': d[f'{prefix}_c'],
            'EX': d[f'{prefix}_ex'],
            'All': d[f'{prefix}_all']
        }
        for d in dat
    }

    tab = pd.DataFrame([data[di] for di in ORDER])
    tab.set_index('Technology', inplace=True)
    print_table(tab)
    # print(tab.style
    #       .format(subset="A", precision=1, thousands=",")
    #       .format(subset="B", precision=1, thousands=",")
    #       .format(subset="C", precision=1, thousands=",")
    #       .format(subset="EX", precision=1, thousands=",")
    #       .format(subset="All", precision=1, thousands=",")
    #       .to_latex(multirow_align="t", multicol_align="r", hrules=True))
    return tab


t1 = tech_count_table('tweets')
t2 = tech_count_table('users')

print_table(t1 / t2, incl_perc=False, precision=2)
