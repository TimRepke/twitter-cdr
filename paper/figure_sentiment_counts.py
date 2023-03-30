import pickle
import datetime
from collections import OrderedDict
from sqlalchemy import text, bindparam, ARRAY, String
import numpy as np
from matplotlib import pyplot as plt
import tikzplotlib
from pathlib import Path
from shared.db import run_query

START = '2006-01-01 00:00'
END = '2022-12-31 23:59'

TECHNOLOGIES = [
    # ('Methane Removal', # 0
    # ('CCS', # 1
    ('Ocean Fertilization', 2),
    ('Ocean Alkalinization', 3),
    ('Enhanced Weathering', 4),
    ('Biochar', 5),
    ('Afforestation/Reforestation', 6),
    ('Ecosystem Restoration', 7),
    ('Soil Carbon Sequestration', 8),
    ('BECCS', 9),
    ('Blue Carbon', 10),
    ('Direct Air Capture', 11),
    ('GGR (general)', 12),
]


def get_tech_data(technology: int | None = None):
    tech_filter = '= :tech' if technology is not None else '> 1'
    stmt = text(f'''
        WITH tmp AS (SELECT ti.item_id,
                            twitter_id,
                            ti.twitter_author_id,
                            (ti.'user' -> 'created_at')::text::timestamp                                        as created,
                            extract('day' from date_trunc('day', :end_time ::timestamp -
                                                                 (ti.'user' -> 'created_at')::text::timestamp)) as days,
                            (ti.'user' -> 'tweet_count')::int                                                   as n_tweets
                     FROM twitter_item ti
                              LEFT JOIN bot_annotation ba_tech ON (
                                 ti.item_id = ba_tech.item_id
                             AND ba_tech.bot_annotation_metadata_id = :ba_tech
                             AND ba_tech.key = 'tech')
                     WHERE ti.project_id = :project_id
                       AND ti.created_at >= :start_time ::timestamp
                       AND ti.created_at <= :end_time ::timestamp
                       AND ba_tech.value_int {tech_filter}),
             users AS (SELECT twitter_author_id,
                              days,
                              n_tweets,
                              COUNT(DISTINCT twitter_id)                                           as n_cdr_tweets,
                              count(DISTINCT tmp.twitter_id) FILTER (WHERE ba_senti.value_int = 2) as 'Positive',
                              count(DISTINCT tmp.twitter_id) FILTER (WHERE ba_senti.value_int = 1) as 'Neutral',
                              count(DISTINCT tmp.twitter_id) FILTER (WHERE ba_senti.value_int = 0) as 'Negative'
                       FROM tmp
                                LEFT JOIN bot_annotation ba_senti ON (
                                   tmp.item_id = ba_senti.item_id
                               AND ba_senti.bot_annotation_metadata_id = :ba_senti
                               AND ba_senti.key = 'senti'
                               AND ba_senti.repeat = 1)
                       GROUP BY twitter_author_id, days, n_tweets),
             users_paneled AS (SELECT users.*,
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
                               FROM users)
        SELECT panel, count(distinct twitter_author_id) as n_users, 
               SUM(n_cdr_tweets) as n_tweets, SUM('Positive') as pos, SUM('Neutral') as neu, SUM('Negative') as neg
        FROM users_paneled uf
        GROUP BY panel
        ORDER BY panel;''')

    print('Running query')
    result = run_query(stmt, {
        'project_id': 'c5d36b2e-cbb4-47a8-8370-e5f52bb78bf3',
        'ba_tech': 'fc73da56-9f51-4d2b-ad35-2a01dbe9b275',
        'ba_senti': 'e63da0c9-9bb5-4026-ab5e-7d5845cdc111',
        'start_time': START,
        'end_time': END,
        'tech': technology
    })
    return result


def gather_data():
    return [
        (t, ti, get_tech_data(ti))
        for t, ti in TECHNOLOGIES + [('Total', None)]
    ]


def make_figure(fname, cache: str | None = None):
    if cache is not None and Path(cache).exists():
        print('Reading data from cache...')
        with open(cache, 'rb') as f_cache:
            data = pickle.load(f_cache)
    else:
        print('Fetching data...')
        data = gather_data()
        if cache is not None:
            with open(cache, 'wb') as f_cache:
                pickle.dump(data, f_cache)

    fig: plt.Figure
    axes: list[plt.Axes]
    fig, axes = plt.subplots(ncols=2, figsize=(15, 8), dpi=150, layout='constrained')  # width/height
    SHOW_NUMS = False
    SHOW_USERS = False
    ax = axes[0]
    techs = [d[0] for d in data]
    stack_n_users = np.array([[int(p['n_users']) for p in d[2]] for d in data])
    stack_n_tweets = np.array([[int(p['n_tweets']) for p in d[2]] for d in data])
    stack_n_pos = np.array([[int(p['pos']) for p in d[2]] for d in data])
    stack_n_neu = np.array([[int(p['neu']) for p in d[2]] for d in data])
    stack_n_neg = np.array([[int(p['neg']) for p in d[2]] for d in data])
    plt.rcParams['hatch.color'] = '#FFFFFF'
    plt.rcParams['hatch.linewidth'] = 0.5
    bottom = np.zeros(stack_n_pos.shape[0])
    patterns = ['/', '\\', '|', '-', '+', 'x', 'o', 'O', '.', '*']
    rel_vals = (stack_n_pos.T / stack_n_tweets[:, :3].sum(axis=1)).T * 100
    for i, p, c in [(0, 'A', '#559E55'), (1, 'B', '#6AC46A'), (2, 'C', '#7FEB7F')]:
        v = rel_vals[:, i]
        b = ax.barh(techs, v, 0.5, left=bottom, color=c, label=p)  # hatch=patterns[i+4],
        if SHOW_NUMS:
            percs = stack_n_pos[:, i] / stack_n_pos[:, :3].sum(axis=1)
            for ri, rect in enumerate(b.patches):
                ax.text(bottom[ri],
                        rect.get_y() + (i * (rect.get_height() / 2)),
                        f'{percs[ri]:.2%}')
        bottom += v

    rel_vals = (stack_n_neg.T / stack_n_tweets[:, :3].sum(axis=1)).T * 100
    bottom = -rel_vals[:, :3].sum(axis=1)
    for i, p, c in [(2, 'C', '#FF905E'), (1, 'B', '#E66360'), (0, 'A', '#BF5250')]:
        v = rel_vals[:, i]
        b = ax.barh(techs, v, 0.5, left=bottom, color=c, label=p)
        if SHOW_NUMS:
            percs = stack_n_neg[:, i] / stack_n_neg[:, :3].sum(axis=1)
            for ri, rect in enumerate(b.patches):
                ax.text(bottom[ri],
                        rect.get_y() + (i * (rect.get_height() / 2)),
                        f'{percs[ri]:.2%}')
        bottom += v
    ax.vlines(0, -1, 12, color='black', lw=1)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['bottom'].set_visible(True)
    ax.spines['left'].set_visible(False)
    ax.tick_params(axis='y', left=False)
    ax.set_ylim(-0.5, 11.5)
    ax.set_title('Sentiments')
    ax.set_xlabel('Share of tweets (%)')

    ax2 = axes[1]
    ax2.set_title('Tweet count')
    ax2.set_xlabel('Number of tweets/users')

    if SHOW_USERS:
        bottom = -stack_n_users[:-1][:, :3].sum(axis=1)
        for i, p, c in [(2, 'C', '#40E0D0'), (1, 'B', '#48D1CC'), (0, 'A', '#00CED1')]:
            v = stack_n_users[:-1][:, i]
            b = ax2.barh(techs[:-1], v, 0.5, left=bottom, color=c, label=p)
            bottom += v


    bottom = np.zeros(stack_n_tweets.shape[0]-1)
    for i, p, c in [(0, 'A', '#00CED1'), (1, 'B', '#48D1CC'), (2, 'C', '#40E0D0')]:
        v = stack_n_tweets[:-1][:, i]
        b = ax2.barh(techs[:-1], v, 0.5, left=bottom, color=c, label=p)
        # percs = stack_n_neg[:, i] / stack_n_neg[:, :3].sum(axis=1)
        # for ri, rect in enumerate(b.patches):
        #     ax.text(bottom[ri],
        #             rect.get_y() + (i * (rect.get_height() / 3)),
        #             f'{percs[ri]:.2%}')
        bottom += v
    if SHOW_NUMS:
        tpu = stack_n_tweets[:-1][:, :3].sum(axis=1)/stack_n_users[:-1][:, :3].sum(axis=1)
        for i, tpui in enumerate(tpu):
            ax2.text(bottom[i]+100, i, f'{tpui:.2f}')

    ax2.vlines(0, -1, 12, color='black', lw=1)
    ax2.spines['top'].set_visible(False)
    ax2.spines['right'].set_visible(False)
    ax2.spines['bottom'].set_visible(True)
    ax2.spines['left'].set_visible(False)
    ax2.set_yticks([])
    ax2.set_ylim(-0.5, 11.5)

    tikzplotlib.clean_figure(fig)
    tikzplotlib.save(
        f'{fname}.tex', fig,
        axis_height='6cm',
        axis_width='14cm',
        extra_axis_parameters=[
            # width=7cm,
            # axis x line*=bottom,
            # y axis line style={opacity=0},
            # ytick style={color=black,opacity=0},
            # xtick pos=left,
            # %ymajorticks=false,
            #
            # and
            #
            # %tick pos=left,
            # width=5cm,
            # axis x line*=bottom,
            # axis y line*=none,
            # ymajorticks=false,
            # xticklabel style={
            #         /pgf/number format/fixed,
            #         /pgf/number format/precision=5
            # },
            # scaled x ticks=false,
            # xtick distance=75000
        ]
    )
    #
    # fig.suptitle('Proportion of tweets per technology per year')
    # fig.legend(loc='outside right upper', reverse=True)

    if not SHOW_USERS:
        ax2.set_xlim(-1000, 180000)

    fig.legend(loc='outside right')
    fig.tight_layout()
    #
    # fig.savefig(f'{fname}.pdf')
    # fig.savefig(f'{fname}.png')
    fig.show()


make_figure(cache='data/sentiment_counts_cache.pkl', fname='figures/sentiments')
