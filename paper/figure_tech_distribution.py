import pickle
import datetime
from collections import OrderedDict
from sqlalchemy import text, bindparam, ARRAY, String
import numpy as np
from matplotlib import pyplot as plt
from matplotlib import ticker
import tikzplotlib
from shared.db import run_query, get_data

START = '2010-01-01 00:00'
END = '2022-12-31 23:59'
BUCKET_SIZE = '1 year'

MAX_USER_TWEETS_PER_DAY = 100
MIN_USER_CDR_TWEETS = 1

print('Loading user data')
with open('data/user_stats.pkl', 'rb') as f:
    data: list[dict] = [r.dict() for r in pickle.load(f)]

print(f'Loaded stats for {len(data):,} users')

print('Prepping filter stats')
user_ids = [d['twitter_author_id'] for d in data]
n_tweets = np.array([d['num_tweets'] for d in data])
lifetime = np.array([(datetime.datetime(2022, 12, 31, 23, 59) - d['created_at']).days for d in data])
tpd = n_tweets / lifetime
n_cdr_tweets = np.array([d['num_cdr_tweets_noccs'] for d in data])
#
# TECHNOLOGIES = [
#     # 'Methane Removal', # 0
#     # 'CCS', # 1
#     'Ocean Fertilization',  # 2
#     'Ocean Alkalinization',  # 3
#     'Enhanced Weathering',  # 4
#     'Biochar',  # 5
#     'Afforestation/Reforestation',  # 6
#     'Ecosystem Restoration',  # 7
#     'Soil Carbon Sequestration',  # 8
#     'BECCS',  # 9
#     'Blue Carbon',  # 10
#     'Direct Air Capture',  # 11
#     'GGR (general)'  # 12
# ]

TECHNOLOGIES = [
    # 'Methane Removal',  # 0
    # 'CCS',  # 1
    'BECCS',  # 9
    'Biochar',  # 5
    'Afforestation/Reforestation',  # 6
    'Ecosystem Restoration',  # 7
    'Soil Carbon Sequestration',  # 8
    'Blue Carbon',  # 10
    'Ocean Fertilization',  # 2
    'Ocean Alkalinization',  # 3
    'Enhanced Weathering',  # 4
    'Direct Air Capture',  # 11
    'GGR (general)',  # 12
]


def make_figure(mask, fname):
    print(f'Mask keeps {sum(mask):,}/{len(mask):,} entries')

    stmt = text('''
                WITH buckets as (SELECT generate_series(:start_time ::timestamp,
                                                        :end_time ::timestamp,
                                                        :bucket_size) as bucket),
                     tweets as (SELECT ut.twitter_id, ut.twitter_author_id, ut.created_at, ba_tech.value_int as technology
                                FROM twitter_item ut
                                         LEFT JOIN bot_annotation ba_tech ON (
                                            ut.item_id = ba_tech.item_id
                                        AND ba_tech.bot_annotation_metadata_id = :ba_tech
                                        AND ba_tech.key = 'tech')
                                WHERE project_id = :project_id),
                     users as (SELECT unnest(:user_ids) as author_id)
                SELECT b.bucket                                                                        as bucket,
                       count(DISTINCT ti.twitter_id)                                                   as num_tweets,
                       -- count(DISTINCT ti.twitter_id) FILTER (WHERE ti.technology = 0)                  as "Methane Removal",
                       -- count(DISTINCT ti.twitter_id) FILTER (WHERE ti.technology = 1)                  as "CCS",
                       count(DISTINCT ti.twitter_id) FILTER (WHERE ti.technology = 2)                  as "Ocean Fertilization",
                       count(DISTINCT ti.twitter_id) FILTER (WHERE ti.technology = 3)                  as "Ocean Alkalinization",
                       count(DISTINCT ti.twitter_id) FILTER (WHERE ti.technology = 4)                  as "Enhanced Weathering",
                       count(DISTINCT ti.twitter_id) FILTER (WHERE ti.technology = 5)                  as "Biochar",
                       count(DISTINCT ti.twitter_id) FILTER (WHERE ti.technology = 6)                  as "Afforestation/Reforestation",
                       count(DISTINCT ti.twitter_id) FILTER (WHERE ti.technology = 7)                  as "Ecosystem Restoration",
                       count(DISTINCT ti.twitter_id) FILTER (WHERE ti.technology = 8)                  as "Soil Carbon Sequestration",
                       count(DISTINCT ti.twitter_id) FILTER (WHERE ti.technology = 9)                  as "BECCS",
                       count(DISTINCT ti.twitter_id) FILTER (WHERE ti.technology = 10)                 as "Blue Carbon",
                       count(DISTINCT ti.twitter_id) FILTER (WHERE ti.technology = 11)                 as "Direct Air Capture",
                       count(DISTINCT ti.twitter_id) FILTER (WHERE ti.technology = 12)                 as "GGR (general)"                                             
                FROM buckets b
                         LEFT OUTER JOIN tweets ti ON (
                            ti.created_at >= b.bucket 
                        AND ti.created_at < (b.bucket + :bucket_size ::interval))
                         LEFT OUTER JOIN users u ON ti.twitter_author_id = u.author_id
                WHERE ti.twitter_author_id = u.author_id AND ti.technology > 1
                GROUP BY b.bucket;
                ''')
    stmt = stmt.bindparams(
        bindparam('user_ids', type_=ARRAY(String), value=[uid for uid, m in zip(user_ids, mask) if m]),
    )

    print('Running query')
    result = get_data(stmt,
                      'data/tech_distribution.pkl',
                      {
                          'project_id': 'c5d36b2e-cbb4-47a8-8370-e5f52bb78bf3',
                          'ba_tech': 'fc73da56-9f51-4d2b-ad35-2a01dbe9b275',
                          'bucket_size': BUCKET_SIZE,
                          'start_time': START,
                          'end_time': END,
                          'user_ids': tuple([uid for uid, m in zip(user_ids, mask) if m])
                      })

    print('Preparing results')
    buckets = [str(r['bucket'].year) for r in result] + ['Total']
    num_tweets = np.array([r['num_tweets'] for r in result])
    stack_totals = np.zeros(len(buckets))
    tech_counts = OrderedDict()
    for technology in TECHNOLOGIES:
        tech_counts[technology] = np.array([r[technology] for r in result])
        tech_counts[technology] = np.hstack([tech_counts[technology], tech_counts[technology].sum()])
        stack_totals += tech_counts[technology]

    plt.rcParams.update({
        'hatch.color': '#FFFFFF',
        'hatch.linewidth': 1,
        # 'text.usetex': True,
        'font.family': 'sans-serif',
        'font.sans-serif': ['Helvetica', 'Liberation Sans'],
        'font.size': 20.0  # default: 10
    })

    print('Making figure')
    fig: plt.Figure
    axes: list[plt.Axes]
    fig, axes = plt.subplots(nrows=2, ncols=1, figsize=(15, 9), dpi=150, layout='constrained')  # width/height
    ax = axes[1]
    cmap = plt.get_cmap('tab20')
    bottom = np.zeros(len(buckets))
    b2 = np.zeros(len(buckets))
    width = 0.5
    checksum = 0

    for ti, technology in enumerate(TECHNOLOGIES):
        normed = (tech_counts[technology] / stack_totals) * 100
        ax.bar(buckets, normed, width, label=technology, bottom=bottom, color=cmap(ti))
        axes[0].bar(buckets[:-1], tech_counts[technology][:-1], width, bottom=b2[:-1], color=cmap(ti))
        bottom += normed
        b2 += tech_counts[technology]
        checksum += tech_counts[technology][:-1].sum()
        # print(f'% {ti} Total: {tech_counts[technology][:-1].sum()} ({technology})')
        # print('%  -> ', tech_counts[technology][:-1])
        # print('%  -> ', normed.astype(int))

        numbers = [f'{c:,} ({n:.0f}\%)' for c, n in zip(tech_counts[technology][:-1], normed)]
        print(f'{technology} & {" & ".join(numbers)} \\\\')
    print('\midrule')
    totals = np.array([tc for tc in tech_counts.values()]).sum(axis=0)
    numbers = [f'{c:,}' for c in totals[:-1]]
    print(f'Total & {" & ".join(numbers)} \\\\')

    for technology in reversed(TECHNOLOGIES):
        rates = tech_counts[technology][1:-1] / tech_counts[technology][:-2]
        numbers = ['---' if np.isnan(r) or np.isinf(r) else f'{r:.2f}' for r in rates]
        print(f'{technology} '
              f'& {" & ".join(numbers)} '
              f'& {np.ma.masked_invalid(rates).mean():.2f} '
              f'({np.ma.masked_invalid(rates).std():.2f}) \\\\')

    totals = np.array([tc for tc in tech_counts.values()]).sum(axis=0)
    rates = totals[1:-1] / totals[:-2]
    numbers = [f'{r:.2f}' for r in rates]
    print(f'Total & {" & ".join(numbers)} & {np.nanmean(rates):.2f} ({np.nanstd(rates):.2f}) \\\\')

    print(f'% Overall checksum of total: {checksum:,}')
    ax.axvline(len(buckets) - 1.5, linewidth=1, color='black')

    tikzplotlib.save(
        f'{fname}.tex', fig,
        axis_height='6cm',
        axis_width='14cm',
        extra_axis_parameters=[
            'x tick label style={/pgf/number format/.cd,fixed,precision=0,set thousands separator={}}',
            'yticklabel style={ /pgf/number format/fixed, /pgf/number format/precision=5}',
            'scaled y ticks=false',
            'legend pos=north west',
            'xmin=0.5', 'xmax=15',
            'ytick distance=20',
            'transpose legend', 'legend columns=3',
            'legend style={at={(0.5,-0.3)},anchor=north, font=\\footnotesize}',
            'cycle multi list={color list\\nextlist [2 of]mark list}'
        ]
    )

    # fig.suptitle('Proportion of tweets per technology per year')
    # fig.legend(loc='outside right upper', reverse=True)

    axes[0].yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: f'{int(x):,}'))
    axes[1].yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: f'{int(x)}%'))
    axes[0].xaxis.set_visible(False)
    axes[1].sharex(axes[0])
    axes[1].xaxis.set_ticks(np.arange(len(buckets)), buckets)
    axes[1].set_xlim(-0.5, 13.5)

    fig.legend(loc='outside lower right', reverse=True, ncols=4, fontsize=18)
    # fig.tight_layout()
    fig.savefig(f'{fname}.pdf')
    fig.savefig(f'{fname}.png')
    fig.show()


make_figure(mask=(tpd <= MAX_USER_TWEETS_PER_DAY) & (n_cdr_tweets >= MIN_USER_CDR_TWEETS),
            fname='figures/tech_distribution')
