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

stmt_double_coded = text('''
WITH tweets AS (SELECT ti.item_id,
                       ti.twitter_id,
                       ti.created_at,
                       ti.twitter_author_id,
                       ba_tech.value_int                                                                   as technology,
                       (ti."user" -> 'created_at')::text::timestamp                                        as created,
                       extract('day' from date_trunc('day', '2023-01-01'::timestamp -
                                                            (ti."user" -> 'created_at')::text::timestamp)) as days,
                       (ti."user" -> 'tweet_count')::int                                                   as n_tweets
                FROM twitter_item ti
                         LEFT JOIN bot_annotation ba_tech ON (
                            ti.item_id = ba_tech.item_id
                        AND ba_tech.bot_annotation_metadata_id = 'fc73da56-9f51-4d2b-ad35-2a01dbe9b275'
                        AND ba_tech.key = 'tech')
                WHERE ti.project_id = 'c5d36b2e-cbb4-47a8-8370-e5f52bb78bf3'
                  AND ti.created_at >= '2010-01-01'::timestamp
                  AND ti.created_at <= '2023-01-01'::timestamp
                  AND ba_tech.value_int > 1),
     users_pre AS (SELECT twitter_author_id,
                          AVG(days)                                                as days,
                          AVG(n_tweets)                                            as n_tweets,
                          COUNT(DISTINCT twitter_id)                               as n_cdr_tweets
                   FROM tweets
                   GROUP BY twitter_author_id),
     dat AS (SELECT ti.created_at        as created_at,
                    ti.twitter_id        as twitter_id,
                    ti.twitter_author_id as twitter_author_id,
                    ti.technology        as technology
             FROM users_pre up
                      LEFT JOIN tweets ti ON ti.twitter_author_id = up.twitter_author_id
             WHERE up.n_tweets / up.days <= 100
               AND up.n_cdr_tweets > :ll
               AND up.n_cdr_tweets <= :ul ),
     techs AS (SELECT DISTINCT technology FROM tweets),
     couples as
         (select d1.technology                 as tec1
               , d2.technology                 as tec2
               , count(distinct d1.twitter_id) as cnt
          from dat d1
                   join dat d2
                        on d1.technology <= d2.technology
                            and d1.twitter_id = d2.twitter_id
          GROUP BY d1.technology, d2.technology)
SELECT t1.technology      as te1,
       t2.technology      as te2,
       coalesce(c.cnt, 0) as cnt
FROM techs t1
         join techs t2 ON t1.technology < t2.technology
         LEFT JOIN couples c ON t1.technology = c.tec1 AND t2.technology = c.tec2;
''')

stmt_tech_counts_tweets = text('''
WITH tweets AS (SELECT ti.item_id,
                       ti.twitter_id,
                       ti.created_at,
                       ti.twitter_author_id,
                       ba_tech.value_int                                                                   as technology,
                       (ti."user" -> 'created_at')::text::timestamp                                        as created,
                       extract('day' from date_trunc('day', '2023-01-01'::timestamp -
                                                            (ti."user" -> 'created_at')::text::timestamp)) as days,
                       (ti."user" -> 'tweet_count')::int                                                   as n_tweets
                FROM twitter_item ti
                         LEFT JOIN bot_annotation ba_tech ON (
                            ti.item_id = ba_tech.item_id
                        AND ba_tech.bot_annotation_metadata_id = 'fc73da56-9f51-4d2b-ad35-2a01dbe9b275'
                        AND ba_tech.key = 'tech')
                WHERE ti.project_id = 'c5d36b2e-cbb4-47a8-8370-e5f52bb78bf3'
                  AND ti.created_at >= '2010-01-01'::timestamp
                  AND ti.created_at <= '2023-01-01'::timestamp
                  AND ba_tech.value_int > 1),
     users_pre AS (SELECT twitter_author_id,
                          AVG(days)                                                as days,
                          AVG(n_tweets)                                            as n_tweets,
                          COUNT(DISTINCT twitter_id)                               as n_cdr_tweets
                   FROM tweets
                   GROUP BY twitter_author_id),
     dat AS (SELECT ti.created_at        as created_at,
                    ti.twitter_id        as twitter_id,
                    ti.twitter_author_id as twitter_author_id,
                    ti.technology        as technology
             FROM users_pre up
                      LEFT JOIN tweets ti ON ti.twitter_author_id = up.twitter_author_id
             WHERE up.n_tweets / up.days <= 100
               AND up.n_cdr_tweets > :ll
               AND up.n_cdr_tweets <= :ul )
SELECT technology, count(DISTINCT twitter_id) as cnt
FROM dat
GROUP BY technology 
''')

stmt_tech_counts = text('''
WITH tweets AS (SELECT ti.item_id,
                       ti.twitter_id,
                       ti.created_at,
                       ti.twitter_author_id,
                       ba_tech.value_int                                                                   as technology,
                       (ti."user" -> 'created_at')::text::timestamp                                        as created,
                       extract('day' from date_trunc('day', '2023-01-01'::timestamp -
                                                            (ti."user" -> 'created_at')::text::timestamp)) as days,
                       (ti."user" -> 'tweet_count')::int                                                   as n_tweets
                FROM twitter_item ti
                         LEFT JOIN bot_annotation ba_tech ON (
                            ti.item_id = ba_tech.item_id
                        AND ba_tech.bot_annotation_metadata_id = 'fc73da56-9f51-4d2b-ad35-2a01dbe9b275'
                        AND ba_tech.key = 'tech')
                WHERE ti.project_id = 'c5d36b2e-cbb4-47a8-8370-e5f52bb78bf3'
                  AND ti.created_at >= '2010-01-01'::timestamp
                  AND ti.created_at <= '2023-01-01'::timestamp
                  AND ba_tech.value_int > 1),
     users_pre AS (SELECT twitter_author_id,
                          AVG(days)                                                as days,
                          AVG(n_tweets)                                            as n_tweets,
                          COUNT(DISTINCT twitter_id)                               as n_cdr_tweets
                   FROM tweets
                   GROUP BY twitter_author_id),
     dat AS (SELECT ti.created_at        as created_at,
                    ti.twitter_id        as twitter_id,
                    ti.twitter_author_id as twitter_author_id,
                    ti.technology        as technology
             FROM users_pre up
                      LEFT JOIN tweets ti ON ti.twitter_author_id = up.twitter_author_id
             WHERE up.n_tweets / up.days <= 100
               AND up.n_cdr_tweets > :ll
               AND up.n_cdr_tweets <= :ul )
SELECT technology, count(DISTINCT twitter_author_id) as cnt
FROM dat
GROUP BY technology 
''')

stmt = text('''
WITH tweets AS (SELECT ti.item_id,
                       ti.twitter_id,
                       ti.created_at,
                       ti.twitter_author_id,
                       ba_tech.value_int                                                                   as technology,
                       (ti."user" -> 'created_at')::text::timestamp                                        as created,
                       extract('day' from date_trunc('day', '2023-01-01'::timestamp -
                                                            (ti."user" -> 'created_at')::text::timestamp)) as days,
                       (ti."user" -> 'tweet_count')::int                                                   as n_tweets
                FROM twitter_item ti
                         LEFT JOIN bot_annotation ba_tech ON (
                            ti.item_id = ba_tech.item_id
                        AND ba_tech.bot_annotation_metadata_id = 'fc73da56-9f51-4d2b-ad35-2a01dbe9b275'
                        AND ba_tech.key = 'tech')
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
     dat AS (SELECT ti.created_at        as created_at,
                    ti.twitter_id        as twitter_id,
                    ti.twitter_author_id as twitter_author_id,
                    ti.technology        as technology
             FROM users_pre up
                      LEFT JOIN tweets ti ON ti.twitter_author_id = up.twitter_author_id
             WHERE up.n_tweets / up.days <= 100
               AND up.n_cdr_tweets > :ll
               AND up.n_cdr_tweets <= :ul)
        ,
     techs AS (SELECT DISTINCT technology FROM dat)
        ,
     couples as
         (select d1.technology                        as tec1
               , d2.technology                        as tec2
               , count(distinct d1.twitter_author_id) as cnt
          from dat d1
                   join dat d2
                        on d1.technology <= d2.technology
                            and d1.twitter_author_id = d2.twitter_author_id
          GROUP BY d1.technology, d2.technology)
SELECT t1.technology      as te1,
       t2.technology      as te2,
       coalesce(c.cnt, 0) as cnt
FROM techs t1
         join techs t2 ON t1.technology < t2.technology
         LEFT JOIN couples c ON t1.technology = c.tec1 AND t2.technology = c.tec2
''')


def data(query, file, ll, ul):
    if file.exists():
        with open(file, 'rb') as fin:
            return pickle.load(fin)
    result = run_query(query, {'ll': ll, 'ul': ul})
    file.parent.mkdir(parents=True, exist_ok=True)
    with open(file, 'wb') as fout:
        pickle.dump(result, fout)
    return result


TECHS = {
    # 0: 'Methane Removal',
    # 1: 'CCS',
    # 2: 'Ocean Fertilization',
    # 3: 'Ocean Alkalinization',
    # 4: 'Enhanced Weathering',
    # 5: 'Biochar',
    # 6: 'Afforestation/Reforestation',
    # 7: 'Ecosystem Restoration',
    # 8: 'Soil Carbon Sequestration',
    # 9: 'BECCS',
    # 10: 'Blue Carbon',
    # 11: 'Direct Air Capture',
    # 12: 'GGR (general)',
    2: 'Ocean\nFertilization',
    3: 'Ocean\nAlkalinization',
    4: 'Enhanced\nWeathering',
    5: 'Biochar',
    6: 'Afforestation/\nReforestation',
    7: 'Ecosystem\nRestoration',
    8: 'Soil Carbon\n Sequestration',
    9: 'BECCS',
    10: 'Blue Carbon',
    11: 'Direct Air\n Capture',
    12: 'GGR (general)'
}
ORDER = [12, 11, 4, 3, 2, 10, 8, 7, 6, 5, 9]
LABELS = [TECHS[o] for o in ORDER]

print('Fetching tweet counts counts B')
tweet_tech_counts_b_dat = data(stmt_tech_counts_tweets, Path('data/tweet_tech_count_panel_b.pkl'), 2, 50)
tweet_tech_counts_b = np.zeros((len(LABELS),))
for row in tweet_tech_counts_b_dat:
    tweet_tech_counts_b[ORDER.index(row['technology'])] = row['cnt']
print(tweet_tech_counts_b)

print('Fetching counts B')
tech_counts_b_dat = data(stmt_tech_counts, Path('data/tech_count_panel_b.pkl'), 2, 50)
tech_counts_b = np.zeros((len(LABELS),))
for row in tech_counts_b_dat:
    tech_counts_b[ORDER.index(row['technology'])] = row['cnt']
print(tech_counts_b)
tech_adjustment_b = 39847 / (11 * tech_counts_b)
print(tech_adjustment_b)

print('Fetching counts C')
tech_counts_c_dat = data(stmt_tech_counts, Path('data/tech_count_panel_c.pkl'), 50, 10000)
tech_counts_c = np.zeros((len(LABELS),))
for row in tech_counts_c_dat:
    tech_counts_c[ORDER.index(row['technology'])] = row['cnt']
print(tech_counts_c)
tech_adjustment_c = 1308 / (11 * tech_counts_c)
print(tech_adjustment_c)

print('Fetching tweet counts counts C')
tweet_tech_counts_c_dat = data(stmt_tech_counts_tweets, Path('data/tweet_tech_count_panel_c.pkl'), 50, 10000)
tweet_tech_counts_c = np.zeros((len(LABELS),))
for row in tweet_tech_counts_c_dat:
    tweet_tech_counts_c[ORDER.index(row['technology'])] = row['cnt']
print(tweet_tech_counts_c)

print('Fetching double coding B')
doubled_b_dat = data(stmt_double_coded, Path('data/tech_double_coding_b.pkl'), 2, 50)
doubled_b = np.zeros((len(LABELS), len(LABELS)))
for row in doubled_b_dat:
    doubled_b[ORDER.index(row['te1']), ORDER.index(row['te2'])] = row['cnt']
np.fill_diagonal(doubled_b, 0)  # reset diagonal to 0
doubled_b = doubled_b + doubled_b.T  # make matrix symmetric

print('Fetching double coding C')
doubled_c_dat = data(stmt_double_coded, Path('data/tech_double_coding_c.pkl'), 50, 10000)
doubled_c = np.zeros((len(LABELS), len(LABELS)))
for row in doubled_c_dat:
    doubled_c[ORDER.index(row['te1']), ORDER.index(row['te2'])] = row['cnt']
np.fill_diagonal(doubled_c, 0)  # reset diagonal to 0
doubled_c = doubled_c + doubled_c.T  # make matrix symmetric

print('Fetching overlaps B')
overlaps_b_dat = data(stmt, Path('data/tech_overlaps_panel_b.pkl'), 2, 50)
overlaps_b = np.zeros((len(LABELS), len(LABELS)))
for row in overlaps_b_dat:
    overlaps_b[ORDER.index(row['te1']), ORDER.index(row['te2'])] = row['cnt']
np.fill_diagonal(overlaps_b, 0)  # reset diagonal to 0
overlaps_b = overlaps_b + overlaps_b.T  # make matrix symmetric
overlaps_b_rel = (overlaps_b.T / tech_counts_b).T
print('Fetching overlaps C')
overlaps_c_dat = data(stmt, Path('data/tech_overlaps_panel_c.pkl'), 50, 10000)
overlaps_c = np.zeros((len(LABELS), len(LABELS)))
for row in overlaps_c_dat:
    overlaps_c[ORDER.index(row['te1']), ORDER.index(row['te2'])] = row['cnt']
np.fill_diagonal(overlaps_c, 0)  # reset diagonal to 0
overlaps_c = overlaps_c + overlaps_c.T  # make matrix symmetric
overlaps_c_rel = (overlaps_c.T / tech_counts_c).T


def draw(values, tit, tr, perc, inte):
    np.fill_diagonal(values, 0)
    plt.rcParams.update({
        'hatch.color': '#FFFFFF',
        'hatch.linewidth': 0.5,
        # 'text.usetex': True,
        # 'font.family': 'sans-serif',
        # 'font.sans-serif': 'Helvetica',
        'font.size': 20.0  # default: 10
    })
    plt.figure(figsize=(12, 12), dpi=150)
    if inte:
        plt.imshow(values, cmap='YlGn')
    else:
        # plt.imshow(values, cmap='YlGn', vmin=0, vmax=0.2)
        plt.imshow(values, cmap='RdYlGn', vmin=-1, vmax=1)
    for (j, i), share in np.ndenumerate(values):
        if i == j:
            continue
        if perc:
            plt.gca().text(i, j, f'{share:.0%}'.replace('%', '\%'),
                           ha='center', va='center', fontsize=16)
        elif inte:
            plt.gca().text(i, j, f'{share:.0f}', ha='center', va='center', fontsize=16)
        else:
            plt.gca().text(i, j, f'{share:.2f}', ha='center', va='center', fontsize=16)
    plt.gca().set_yticks(np.arange(len(LABELS)), LABELS)
    if tr:
        plt.gca().yaxis.tick_right()
        plt.gca().set_xticks(np.arange(len(LABELS)), LABELS, rotation=-40, ha='left')
    else:
        plt.gca().set_xticks(np.arange(len(LABELS)), LABELS, rotation=40, ha='right')
    plt.gca().invert_yaxis()
    plt.title(tit)
    plt.tight_layout()


# print('Drawing B')
# draw(overlaps_b_dat, overlaps_b_rel, '3-50 tweets', tr=False, perc=True)
# plt.savefig(f'figures/technology_user_overlap_b.pdf')
# plt.show()
#
# print('Drawing C')
# draw(overlaps_c_dat, overlaps_c_rel, '>50 tweets', tr=True, perc=True)
# plt.savefig(f'figures/technology_user_overlap_c.pdf')
# plt.show()
print(doubled_b)
print('db mean', doubled_b.mean())
print('db med', np.median(doubled_b))
print('Drawing normed B')
expected = np.zeros((11, 11))
# tpu = tweet_tech_counts_b / tech_counts_b
EPS = 1e-10
for i in range(11):
    for j in range(11):
        TOTAL = 39847
        proba_i = tech_counts_b[i] / TOTAL
        proba_j = tech_counts_b[j] / TOTAL

        tpu_i = tweet_tech_counts_b[i] / (tech_counts_b[i] + 1)
        tpu_j = tweet_tech_counts_b[j] / (tech_counts_b[j] + 1)
        # prop_multi = tweet_tech_counts_b[i] / (doubled_b[i, j]+1)
        # overlap_adjust = prop_multi/tpu

        # # expected[i, j] = proba_i * proba_j * TOTAL * overlap_adjust
        # overlap_adjust = doubled_b[i, j] / (tweet_tech_counts_b[i]+1)
        # # overlap_adjust = doubled_b[i, j] / (doubled_b.sum() / 2)
        # # overlap_adjust *= tweet_tech_counts_b[i]
        # overlap_adjust /= (tpu_i+tpu_j)
        # # overlap_adjust = 0
        #
        # expected[i, j] = (proba_i * proba_j) * TOTAL + overlap_adjust*TOTAL

        overlap_adjust = doubled_b[i, j] / (doubled_b.sum() / 2)
        overlap_adjust /= tpu_i
        overlap_adjust /= tpu_j
        # expected[i, j] = ((proba_i * proba_j) + overlap_adjust) * (TOTAL - doubled_b.sum() / 2)
        # expected[i, j] = ((proba_i * proba_j) + overlap_adjust) * TOTAL
        expected[i, j] = proba_i * proba_j * TOTAL + overlap_adjust * TOTAL
        print(i, j, overlap_adjust)

        # expected[i, j] = (tweet_tech_counts_b[i] / 39847) * (tweet_tech_counts_b[j] / 39847) * 39847
        # expected[i, j] = (tech_counts_b[i] / 39847) * (tech_counts_b[j] / 39847) * 39847 * (doubled_b.max() / doubled_b[i, j])
        # expected[i, j] = (((tech_counts_b[i] / 39847) * (tech_counts_b[j] / 39847)) + (
        #             doubled_b[i, j] / (doubled_b.sum() / 2))) * 39847
        # expected[i, j] = (tech_counts_b[i] / 39847) * (tech_counts_b[j] / 39847) * 39847 * (tpu.max()/tpu[i]) * (tpu.max()/tpu[j])
        # expected[i, j] = (tweet_tech_counts_b[i] / tweet_tech_counts_b.sum()) * (tweet_tech_counts_b[j] / tweet_tech_counts_b.sum()) * tweet_tech_counts_b.sum()
# mat = tech_counts_b.repeat(len(TECHS)).reshape((len(TECHS),-1)).T
# dst = (tech_counts_b.T / 39847).T
# m1 = dst.repeat(len(TECHS)).reshape((len(TECHS), -1)).T
# m2 = (m1.T / dst).T
# print(m2)
# expected = (m1*m1.T).T * 39847
# # expected = m2 * 39847
# print(overlaps_b.astype(int))
print(expected.astype(int))

print('ob sum', overlaps_b.sum())
print('e sum', expected.sum())
print('lol', (expected * (overlaps_b.sum() / expected.sum())).sum())

# draw(((overlaps_b - expected) / expected), '>50 tweets', tr=False, perc=False,inte=False)
# plt.show()
# draw(np.log(overlaps_b  / expected), '>50 tweets', tr=False, perc=False,inte=False)
# draw(overlaps_b  / expected, '>50 tweets', tr=False, perc=False,inte=False)
draw((overlaps_b / expected) - 1, 'overlap/expected', tr=False, perc=False, inte=False)
# draw((overlaps_b / (expected*(overlaps_b.sum()/expected.sum())))-1, '>50 tweets', tr=False, perc=False, inte=False)
# # plt.savefig(f'figures/technology_user_overlap_c.pdf')
plt.show()

draw(overlaps_b-expected, '>50 tweets', tr=False, perc=False, inte=True)
# draw((overlaps_b / (expected*(overlaps_b.sum()/expected.sum())))-1, '>50 tweets', tr=False, perc=False, inte=False)
# # plt.savefig(f'figures/technology_user_overlap_c.pdf')
plt.show()

draw(expected, '>50 tweets', tr=False, perc=False, inte=True)
plt.show()
draw(overlaps_b, '>50 tweets', tr=False, perc=False, inte=True)
plt.show()

# draw(doubled_c, '>50 tweets', tr=False, perc=False, inte=True)
# plt.show()
# draw(doubled_b, '>50 tweets', tr=False, perc=False, inte=True)
# plt.show()

# print('Drawing C-B')
# draw(overlaps_c_dat, overlaps_c_rel-overlaps_b_rel, 'diff', tr=False, perc=True)
# plt.savefig(f'figures/technology_user_overlap_diff.pdf')
# plt.show()

# print('Drawing B')
# draw(overlaps_b_dat, overlaps_b_rel * tech_adjustment_b, '3-50 tweets', tr=False, perc=False)
# plt.savefig(f'figures/technology_user_overlap_b_adjusted.pdf')
# plt.show()

# print('Drawing C')
# draw(overlaps_c_dat, overlaps_c_rel * tech_adjustment_c, '>50 tweets', tr=True, perc=False)
# plt.savefig(f'figures/technology_user_overlap_c_adjusted.pdf')
# plt.show()
