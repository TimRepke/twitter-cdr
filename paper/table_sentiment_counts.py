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
from shared.db import run_query

START = '2010-01-01 00:00'
END = '2022-12-31 23:59'

TECHS = {
    # 0: 'Methane Removal',
    # 1: 'CCS',
    2: 'Ocean Fertilization',
    3: 'Ocean Alkalinization',
    4: 'Enhanced Weathering',
    5: 'Biochar',
    6: 'Afforestation - Reforestation',
    7: 'Ecosystem Restoration',
    8: 'Soil Carbon Sequestration',
    9: 'BECCS',
    10: 'Blue Carbon',
    11: 'Direct Air Capture',
    12: 'GGR (general)',
    100: 'Total',
    # 200: 'Total (incl. CCS\&MR)'
}

# ORDER = [12, 11, 4, 3, 2, 10, 8, 7, 6, 5, 9, 100, 1, 0, 200]
ORDER = [12, 11, 4, 3, 2, 10, 8, 7, 6, 5, 9, 100]


def get_data(cache: Path | None = None):
    if cache and cache.exists():
        with open(cache, 'rb') as fin:
            return pickle.load(fin)

    else:
        stmt = text(f'''
           WITH buckets as (SELECT generate_series('2010-01-01 00:00'::timestamp,
                                                   '2022-12-31 00:00'::timestamp,
                                                   '3 months') as bucket),
                 technologies as (SELECT DISTINCT value_int as technology
                                  FROM bot_annotation
                                  WHERE bot_annotation_metadata_id = 'fc73da56-9f51-4d2b-ad35-2a01dbe9b275'
                                    AND key = 'tech'
                                    AND value_int > 1),
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
                 dat as (SELECT ti.created_at as created_at,
                                ti.twitter_id as twitter_id,
                                ti.sentiment  as sentiment,
                                ti.technology as technology,
                                u.panel       as panel
                         FROM tweets ti
                                  LEFT OUTER JOIN users u ON ti.twitter_author_id = u.twitter_author_id)
            SELECT b.bucket                                                                        as bucket
                 , t.technology                                                                    as technology
                 , count(DISTINCT d.twitter_id)                                                    as tweets_all
                 , count(DISTINCT d.twitter_id) filter ( where d.sentiment = 2 )                   as tweets_all_pos
                 , count(DISTINCT d.twitter_id) filter ( where d.sentiment = 1 )                   as tweets_all_neu
                 , count(DISTINCT d.twitter_id) filter ( where d.sentiment = 0 )                   as tweets_all_neg
                 , count(DISTINCT d.twitter_id) filter ( where d.panel = 'A')                      as tweets_a
                 , count(DISTINCT d.twitter_id) filter ( where d.panel = 'A' AND d.sentiment = 2)  as tweets_a_pos
                 , count(DISTINCT d.twitter_id) filter ( where d.panel = 'A' AND d.sentiment = 1)  as tweets_a_neu
                 , count(DISTINCT d.twitter_id) filter ( where d.panel = 'A' AND d.sentiment = 0 ) as tweets_a_neg
                 , count(DISTINCT d.twitter_id) filter ( where d.panel = 'B')                      as tweets_b
                 , count(DISTINCT d.twitter_id) filter ( where d.panel = 'B' AND d.sentiment = 2)  as tweets_b_pos
                 , count(DISTINCT d.twitter_id) filter ( where d.panel = 'B' AND d.sentiment = 1)  as tweets_b_neu
                 , count(DISTINCT d.twitter_id) filter ( where d.panel = 'B' AND d.sentiment = 0 ) as tweets_b_neg
                 , count(DISTINCT d.twitter_id) filter ( where d.panel = 'C')                      as tweets_c
                 , count(DISTINCT d.twitter_id) filter ( where d.panel = 'C' AND d.sentiment = 2)  as tweets_c_pos
                 , count(DISTINCT d.twitter_id) filter ( where d.panel = 'C' AND d.sentiment = 1)  as tweets_c_neu
                 , count(DISTINCT d.twitter_id) filter ( where d.panel = 'C' AND d.sentiment = 0 ) as tweets_c_neg
            FROM (buckets b CROSS JOIN technologies t)
                     LEFT JOIN dat d ON (
                        d.created_at >= b.bucket
                    AND d.created_at <= (b.bucket + '3 month'::interval)
                    AND t.technology = d.technology)
            GROUP BY t.technology, b.bucket
            UNION
            SELECT b.bucket                                                                        as bucket
                 , 100                                                                             as technology
                 , count(DISTINCT d.twitter_id)                                                    as tweets_all
                 , count(DISTINCT d.twitter_id) filter ( where d.sentiment = 2 )                   as tweets_all_pos
                 , count(DISTINCT d.twitter_id) filter ( where d.sentiment = 1 )                   as tweets_all_neu
                 , count(DISTINCT d.twitter_id) filter ( where d.sentiment = 0 )                   as tweets_all_neg
                 , count(DISTINCT d.twitter_id) filter ( where d.panel = 'A')                      as tweets_a
                 , count(DISTINCT d.twitter_id) filter ( where d.panel = 'A' AND d.sentiment = 2)  as tweets_a_pos
                 , count(DISTINCT d.twitter_id) filter ( where d.panel = 'A' AND d.sentiment = 1)  as tweets_a_neu
                 , count(DISTINCT d.twitter_id) filter ( where d.panel = 'A' AND d.sentiment = 0 ) as tweets_a_neg
                 , count(DISTINCT d.twitter_id) filter ( where d.panel = 'B')                      as tweets_b
                 , count(DISTINCT d.twitter_id) filter ( where d.panel = 'B' AND d.sentiment = 2)  as tweets_b_pos
                 , count(DISTINCT d.twitter_id) filter ( where d.panel = 'B' AND d.sentiment = 1)  as tweets_b_neu
                 , count(DISTINCT d.twitter_id) filter ( where d.panel = 'B' AND d.sentiment = 0 ) as tweets_b_neg
                 , count(DISTINCT d.twitter_id) filter ( where d.panel = 'C')                      as tweets_c
                 , count(DISTINCT d.twitter_id) filter ( where d.panel = 'C' AND d.sentiment = 2)  as tweets_c_pos
                 , count(DISTINCT d.twitter_id) filter ( where d.panel = 'C' AND d.sentiment = 1)  as tweets_c_neu
                 , count(DISTINCT d.twitter_id) filter ( where d.panel = 'C' AND d.sentiment = 0 ) as tweets_c_neg
            FROM buckets b
                     LEFT JOIN dat d ON (
                        d.created_at >= b.bucket
                    AND d.created_at <= (b.bucket + '3 month'::interval))
            GROUP BY b.bucket
            ORDER BY 2, 1;''')

        print('Running query')
        result = run_query(stmt, {
            'project_id': 'c5d36b2e-cbb4-47a8-8370-e5f52bb78bf3',
            'ba_tech': 'fc73da56-9f51-4d2b-ad35-2a01dbe9b275',
            'ba_senti': 'e63da0c9-9bb5-4026-ab5e-7d5845cdc111',
            'start_time': START,
            'end_time': END
        })

        with open(cache, 'wb') as fout:
            pickle.dump(result, fout)

    return result


dat = get_data(cache=Path('data/panel_sentiments.pkl'))
parsed_dat = [
    {
        'bucket': d['bucket'],
        'ti': int(d['technology']),
        'Technology': TECHS[int(d['technology'])],
        **{
            f'{p}{p_}': d[f'tweets_{f}{f_}']
            for f, p in [('a', 'A'), ('b', 'B'), ('c', 'C'), ('all', 'All')]
            for p_, f_ in [('+', '_pos'), ('0', '_neu'), ('-', '_neg'), ('', '')]
        }
    }
    for d in dat
]
full_tab = pd.DataFrame(parsed_dat)
full_tab['bucket_dt'] = [dt.date() for dt in full_tab['bucket']]  # strftime('%Y-%m-%d')

tab = full_tab \
    .drop(columns=['bucket', 'ti', 'bucket_dt']) \
    .groupby('Technology') \
    .sum() \
    .loc[[TECHS[o] for o in ORDER]]
tab = tab.reset_index()
tab.set_index('Technology', inplace=True)

pd.options.display.max_columns = None
pd.options.display.max_rows = None
pd.options.display.expand_frame_repr = False

acc_med = []
acc_avg = []
acc_std = []

latex = []

with pd.ExcelWriter('figures/sentiments/stats.xlsx') as writer:
    for tech in list(tab.index):
        timeline = full_tab[full_tab['Technology'] == tech]
        timeline = timeline.set_index('bucket_dt')
        timeline['yr'] = [r['bucket'].year for _, r in timeline.iterrows()]
        ttab = timeline.reset_index()[['yr',
                                       'A+', 'A0', 'A-', 'A',
                                       'B+', 'B0', 'B-', 'B',
                                       'C+', 'C0', 'C-', 'C',
                                       'All+', 'All0', 'All-', 'All']].groupby('yr').sum()
        ttab.to_excel(writer, sheet_name=tech)

        tmp = ttab[1:].values / ttab[:-1].values
        tmp[np.isinf(tmp)] = np.nan
        tmp = pd.DataFrame(tmp, index=ttab.index[1:], columns=ttab.columns).reset_index()

        med = ['med'] + tmp.median(axis=0).to_list()[1:]
        avg = ['avg'] + tmp.mean(axis=0).to_list()[1:]
        std = ['std'] + tmp.std(axis=0).to_list()[1:]

        latex.append((tech, tmp['All'], med[-1], avg[-1], std[-1]))

        tmp.loc[12] = med
        tmp.loc[13] = avg
        tmp.loc[14] = std
        tmp = tmp.set_index('yr')
        tmp.to_excel(writer, sheet_name=tech, startrow=16, float_format="%.2f")
        acc_med.append(tmp.loc['med'])
        acc_avg.append(tmp.loc['avg'])
        acc_std.append(tmp.loc['std'])

    tab_med = pd.DataFrame(acc_med, index=tab.index, columns=tmp.columns)
    tab_avg = pd.DataFrame(acc_avg, index=tab.index, columns=tmp.columns)
    tab_std = pd.DataFrame(acc_std, index=tab.index, columns=tmp.columns)
    tab_med.to_excel(writer, sheet_name='Averages', float_format="%.2f")
    tab_avg.to_excel(writer, sheet_name='Averages', startrow=16, float_format="%.2f")
    tab_std.to_excel(writer, sheet_name='Averages', startrow=32, float_format="%.2f")

print('\\toprule')
print('Method & ', end='')
for yr in range(2010, 2022, 1):
    print(f'{yr}--{yr + 1} & ', end='')
print('median (std)\\\\\\midrule')
for tech, nums, med, avg, std in latex:
    print(tech, end=' & ')
    for num in nums:
        print(f'{num:.2f}', end=' & ')
    print(f'{med:.2f} ({std:.2f}) \\\\')
print('\\bottomrule')




