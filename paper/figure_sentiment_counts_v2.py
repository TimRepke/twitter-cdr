import pickle
import pandas as pd
from matplotlib.gridspec import GridSpec

import matplotlib.dates as mdates
from sqlalchemy import text, bindparam, ARRAY, String
import numpy as np
from matplotlib import pyplot as plt
import tikzplotlib
from pathlib import Path
from shared.db import run_query

START = '2006-01-01 00:00'
END = '2022-12-31 23:59'

TECHS = {
    # 0: 'Methane Removal',
    # 1: 'CCS',
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

print(tab)

techs = list(tab.index)
stack_n_pos = tab[['A+', 'B+', 'C+', 'All+']]
stack_n_neu = tab[['A0', 'B0', 'C0', 'All0']]
stack_n_neg = tab[['A-', 'B-', 'C-', 'All-']]
stack_n_tot = tab[['A', 'B', 'C', 'All']]


def figure_rel_all():
    print(stack_n_pos_rel)
    print(stack_n_neg_rel)

    plt.rcParams['hatch.color'] = '#FFFFFF'
    plt.rcParams['hatch.linewidth'] = 0.5

    bottom = np.zeros(stack_n_pos.shape[0])
    for col, name, colour in [('A+', 'A', '#559E55'), ('B+', 'B', '#6AC46A'), ('C+', 'C', '#7FEB7F')]:
        values = stack_n_pos_rel[col].to_numpy() * 100
        b = ax.barh(techs, values, 0.5, left=bottom, color=colour, label=name)  # hatch=patterns[i+4],
        bottom += values

    bottom = -stack_n_neg_rel[['A-', 'B-', 'C-']].sum(axis=1) * 100
    for col, name, colour in [('C-', 'C', '#FF905E'), ('B-', 'B', '#E66360'), ('A-', 'A', '#BF5250')]:
        values = stack_n_neg_rel[col].to_numpy() * 100
        b = ax.barh(techs, values, 0.5, left=bottom, color=colour, label=name)  # hatch=patterns[i+4],
        bottom += values

    fig.legend(loc='outside right')
    fig.tight_layout()
    fig.show()


GREENS = ['#559E55', '#6AC46A', '#7FEB7F']
REDS = ['#BF5250', '#E66360', '#FF905E']


def plt_fgr(ax: plt.Axes, xlim=None, bars_rel=True):
    stack_n_pos_rel.fillna(0, inplace=True)
    stack_n_neu_rel.fillna(0, inplace=True)
    stack_n_neg_rel.fillna(0, inplace=True)
    print(stack_n_pos_rel)
    print(stack_n_neg_rel)

    patterns = ['/', '\\', '|', '-', '+', 'x', 'o', 'O', '.', '*']
    plt.rcParams['hatch.color'] = '#FFFFFF'
    plt.rcParams['hatch.linewidth'] = 0.5

    BAR_WIDTH = 0.2
    for off, col, name, colour in [(-BAR_WIDTH - 0.05, 'A+', 'A', GREENS[0]),
                                   (0, 'B+', 'B', GREENS[1]),
                                   (BAR_WIDTH + 0.05, 'C+', 'C', GREENS[2])]:
        values = stack_n_pos_rel[col].to_numpy() * 100
        b = ax.barh(np.arange(len(techs)) + off, values, BAR_WIDTH, color=colour, label=name)
        if SHOW_NUMS:
            for ri, rect in enumerate(b.patches):
                ax.text(values[ri],
                        rect.get_y() + BAR_WIDTH,
                        f'{values[ri]:.1f}\%')

    for off, col, name, colour in [(-BAR_WIDTH - 0.05, 'A-', 'A', REDS[0]),
                                   (0, 'B-', 'B', REDS[1]),
                                   (BAR_WIDTH + 0.05, 'C-', 'C', REDS[2])]:
        values = stack_n_neg_rel[col].to_numpy() * -100
        b = ax.barh(np.arange(len(techs)) + off, -values, BAR_WIDTH, left=values, color=colour,
                    label=name)
        if SHOW_NUMS:
            for ri, rect in enumerate(b.patches):
                ax.text(values[ri],
                        rect.get_y() + BAR_WIDTH,
                        f'{(-1) * values[ri]:.1f}\%',
                        horizontalalignment='right')

    ax.vlines(0, -0.5, len(techs) - 0.5, color='black', lw=1)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['bottom'].set_visible(True)
    ax.spines['left'].set_visible(False)
    ax.tick_params(axis='y', left=False)
    ax.set_yticks(np.arange(len(techs)), techs, fontsize='x-large')
    ax.set_ylim(-0.5, len(techs) - 0.5)
    if xlim is not None:
        ax.set_xlim(*xlim)
    # ax.set_title('Sentiments')
    ax.set_xlabel('Share of tweets (\%)')
    ax.invert_yaxis()

    subgrid = outer_grid[0, 1].subgridspec(nrows=len(TECHS), ncols=3, wspace=0, hspace=0)

    for ti, tech in enumerate(list(tab.index)):
        timeline = full_tab[full_tab['Technology'] == tech]
        timeline = timeline.set_index('bucket_dt')
        xs = timeline.index
        xs_ = np.arange(len(xs))

        for gi, group in enumerate(['A', 'B', 'C']):
            ax = fig.add_subplot(subgrid[ti, gi])
            subax = ax.twinx()
            ax.spines.top.set_visible(False)
            ax.spines.right.set_visible(False)
            ax.spines.bottom.set_visible(False)
            ax.spines.left.set_visible(False)

            if bars_rel:
                bpos = (-1) * (timeline[f'{group}+'] / timeline[group])
                bpos.fillna(0, inplace=True)
                bneg = timeline[f'{group}-'] / timeline[group]
                bneg.fillna(0, inplace=True)
                ax.bar(xs, bpos, width=72, bottom=1, color=GREENS[gi])
                ax.bar(xs, bneg, width=72, color=REDS[gi])
                ax.set_ylim(0, 1)
                subax.plot(xs, timeline[group])
                subax.set_ylim(0, timeline[['A', 'B', 'C']].to_numpy().max())
            else:
                mx = (timeline[f'{group}+'] + timeline[f'{group}-']).max()
                # ax.hlines(0, 0, len(xs), color='grey', lw=1, ls=':')
                ax.bar(xs, -timeline[f'{group}+'], width=72, bottom=mx, color=GREENS[gi])
                ax.bar(xs, timeline[f'{group}-'], width=72, color=REDS[gi])
                mx = timeline[[f'{group}+', f'{group}-']].to_numpy().max()
                ax.set_ylim(0, mx)
                subax.plot(xs, timeline[group])
                subax.set_ylim(0, timeline[['A', 'B', 'C']].to_numpy().max())

            ax.set_xlim(xs[0], xs[-1])
            if tech == 'Total':
                ax.set_yticks([])
                subax.set_yticks([])
                ax.xaxis.set_major_locator(mdates.MonthLocator(interval=24))
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
                ax.xaxis.set_tick_params(rotation=40)
            else:
                subax.set(xticks=[], yticks=[])
                ax.set(xticks=[], yticks=[])
    # fig.legend(loc='outside right')
    # # fig.tight_layout()
    #
    # fig.show()


TARGET = 'figures/sentiments'
SHOW_NUMS = True
plt.rcParams.update({
    'text.usetex': True,
    'font.family': 'serif'
})

print('Normalise to all tweets')
stack_n_pos_rel = pd.DataFrame(stack_n_pos.to_numpy() / stack_n_tot['All'].to_numpy().reshape((-1, 1)),
                               index=stack_n_pos.index, columns=stack_n_pos.columns)
stack_n_neu_rel = pd.DataFrame(stack_n_neu.to_numpy() / stack_n_tot['All'].to_numpy().reshape((-1, 1)),
                               index=stack_n_neu.index, columns=stack_n_neu.columns)
stack_n_neg_rel = pd.DataFrame(stack_n_neg.to_numpy() / stack_n_tot['All'].to_numpy().reshape((-1, 1)),
                               index=stack_n_neg.index, columns=stack_n_neg.columns)

# figure_rel_all()
fig: plt.Figure
axes: list[plt.Axes]
fig = plt.figure(figsize=(15, 8), dpi=150, layout='constrained')  # width/height
fig.subplots_adjust(hspace=0, wspace=0)
outer_grid = fig.add_gridspec(nrows=1, ncols=2, wspace=0, hspace=0, width_ratios=[1 / 3, 2 / 3])
ax = fig.add_subplot(outer_grid[0, 0])
plt_fgr(xlim=(-11, 21), ax=ax, bars_rel=True)

# tikzplotlib.clean_figure(fig)

plt.savefig(f'{TARGET}/sentiments_totalnorm.pdf')
fig.show()

fig.clf()
plt.clf()

print('Normalise to respective user category')
stack_n_pos_rel = pd.DataFrame(stack_n_pos.to_numpy() / stack_n_tot.to_numpy(),
                               index=stack_n_pos.index, columns=stack_n_pos.columns)
stack_n_neu_rel = pd.DataFrame(stack_n_neu.to_numpy() / stack_n_tot.to_numpy(),
                               index=stack_n_neu.index, columns=stack_n_neu.columns)
stack_n_neg_rel = pd.DataFrame(stack_n_neg.to_numpy() / stack_n_tot.to_numpy(),
                               index=stack_n_neg.index, columns=stack_n_neg.columns)
fig = plt.figure(figsize=(15, 8), dpi=150, layout='constrained')  # width/height
outer_grid = fig.add_gridspec(nrows=1, ncols=2, wspace=0, hspace=0, width_ratios=[1 / 3, 2 / 3])
ax = fig.add_subplot(outer_grid[0, 0])

plt_fgr(xlim=(-25, 42), ax=ax, bars_rel=False)
plt.savefig(f'{TARGET}/sentiments_selfnorm.pdf')
fig.show()
