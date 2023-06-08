import pickle
import datetime

from sqlalchemy import text, bindparam, ARRAY, String
import numpy as np
from matplotlib import pyplot as plt
import tikzplotlib

from shared.models import UserTweetCounts, row_to_obj
from shared.db import run_query

START = '2006-01-01 00:00'
END = '2022-12-31 23:59'
BUCKET_SIZE = '1 year'

MAX_USER_TWEETS_PER_DAY = 100
MIN_USER_CDR_TWEETS = 2

print('Loading user data')
with open('data/user_stats.pkl', 'rb') as f:
    # data: list[UserTweetCounts] = [row_to_obj(d) for d in pickle.load(f)]
    data: list[UserTweetCounts] = pickle.load(f)

print(f'Loaded stats for {len(data):,} users')

print('Prepping filter stats')
user_ids = [d.twitter_author_id for d in data]
n_tweets = np.array([d.num_tweets for d in data])
lifetime = np.array([(datetime.datetime(2022, 12, 31, 23, 59) - d.created_at).days for d in data])
tpd = n_tweets / lifetime
n_cdr_tweets = np.array([d.num_cdr_tweets for d in data])
n_cdr_tweets_noccs = np.array([d.num_cdr_tweets_noccs for d in data])


def get_data(mask):
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
                    SELECT b.bucket                                                                          as bucket,
                           count(DISTINCT ti.twitter_id)                                                     as num_tweets_all,
                           count(DISTINCT ti.twitter_id) FILTER ( WHERE ti.technology > 1 )                  as num_tweets_noccs,
                           count(DISTINCT ti.twitter_id) FILTER ( WHERE ti.twitter_author_id = u.author_id ) as num_tweets_filtered,
                           count(DISTINCT ti.twitter_id) FILTER ( WHERE ti.twitter_author_id = u.author_id 
                                                                        AND ti.technology > 1)               as num_tweets_filtered_noccs
                    FROM buckets b
                             LEFT OUTER JOIN tweets ti ON (
                                ti.created_at >= (b.bucket - :bucket_size ::interval)
                            AND ti.created_at < b.bucket)
                             LEFT OUTER JOIN users u ON ti.twitter_author_id = u.author_id
                    GROUP BY b.bucket;
                    ''')
    stmt = stmt.bindparams(
        bindparam('user_ids', type_=ARRAY(String), value=[uid for uid, m in zip(user_ids, mask) if m]),
    )

    print('Running query')
    result = run_query(stmt, {
        'project_id': 'c5d36b2e-cbb4-47a8-8370-e5f52bb78bf3',
        'ba_tech': 'fc73da56-9f51-4d2b-ad35-2a01dbe9b275',
        'bucket_size': BUCKET_SIZE,
        'start_time': START,
        'end_time': END,
        'user_ids': tuple([uid for uid, m in zip(user_ids, mask) if m])
    })
    return result


def make_figure(result, fname):
    print('Preparing results')
    buckets = [r['bucket'].year for r in result]  # FIXME assumes we always query for bucket_size = year
    num_tweets_all = np.array([r['num_tweets_all'] for r in result])
    num_tweets_noccs = np.array([r['num_tweets_noccs'] for r in result])
    num_tweets_filtered = np.array([r['num_tweets_filtered'] for r in result])
    num_tweets_filtered_noccs = np.array([r['num_tweets_filtered_noccs'] for r in result])

    print('Computing sub-stacks')
    noccs = num_tweets_all - num_tweets_noccs
    filtered = num_tweets_all - num_tweets_filtered_noccs
    full = num_tweets_all - noccs - filtered

    print(noccs)
    print(filtered)
    print(full)

    print('Making figure')
    fig: plt.Figure
    axes: list[list[plt.Axes]]
    fig, ax = plt.subplots(figsize=(15, 8), dpi=150)  # width/height

    bottom = np.zeros(len(buckets))
    width = 0.5
    ax.bar(buckets, filtered, width, label='Analysed tweets', bottom=bottom, color='#FF7F0E')
    bottom += filtered
    ax.bar(buckets, noccs, width, label='Excluded tweets', bottom=bottom, color='#ffad66')
    bottom += noccs
    ax.bar(buckets, full, width, label='CCS tweets', bottom=bottom, color='#ffc999')
    tikzplotlib.save(
        f'{fname}.tex', fig,
        axis_height='6cm',
        axis_width='14cm',
        extra_axis_parameters=[
            'x tick label style={/pgf/number format/.cd,fixed,precision=0,set thousands separator={}}',
            'yticklabel style={ /pgf/number format/fixed, /pgf/number format/precision=5}',
            'scaled y ticks=false',
            'legend pos=north west',
            'xmin=2009.5, xmax=2022.5'
        ]
    )

    ax.set_title('Number of CDR-related tweets per year')
    ax.legend(loc="upper left")
    fig.tight_layout()

    ax.set_xlim((2009.5, 2022.5))
    fig.savefig(f'{fname}.pdf')
    fig.savefig(f'{fname}.png')
    fig.show()


dat = get_data(mask=(tpd <= MAX_USER_TWEETS_PER_DAY) & (n_cdr_tweets >= MIN_USER_CDR_TWEETS))
make_figure(result=dat, fname='figures/yearly_tweets')

dat = get_data(mask=(tpd <= MAX_USER_TWEETS_PER_DAY) & (n_cdr_tweets_noccs >= MIN_USER_CDR_TWEETS))
make_figure(result=dat, fname='figures/yearly_tweets_noccs')
