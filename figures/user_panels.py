import uuid
from pathlib import Path

from common.vector_index import VectorIndex
from common.config import settings
from common.queries import queries
import datetime
from sqlalchemy import text

from nacsos_data.db import get_engine, DatabaseEngine
import numpy as np
from common.config import settings
from common.db_cache import QueryCache
from common.queries import queries
from matplotlib import pyplot as plt
from matplotlib import dates as mdates
from scipy.interpolate import interp1d


def smooth(array, kernel_size, with_pad=True):
    kernel = np.ones(kernel_size) / kernel_size
    if with_pad:
        padded = np.pad(array, kernel_size // 2, mode='edge')
        smoothed = np.convolve(padded, kernel, mode='same')
        return np.array(smoothed).T[kernel_size // 2:-kernel_size // 2].T
    return np.convolve(array, kernel, mode='valid')


class Vectors:
    def __init__(self, file: str):
        # Load tSNE vectors
        index = VectorIndex()
        index.load(Path(file))
        self.id2idx = {item_id: idx for idx, item_id in index.dict_labels.items()}
        self.x = index.vectors[:, 0]
        self.y = index.vectors[:, 1]
        del index

    def get_xy(self, item_id: str | uuid.UUID) -> tuple[float, float]:
        return self.x[self.id2idx[str(item_id)]], self.y[self.id2idx[str(item_id)]]


class Container:
    def __init__(self,
                 technology_int: int | None, technology: str,
                 vectors: Vectors, engine: DatabaseEngine,
                 time_from: datetime.datetime, time_to: datetime.datetime, project_id: str,
                 verbose: bool = False):
        if verbose: print(f'Building container for {technology} ({technology_int})...')
        self.technology = technology_int  # if None, falling back to tech > 1 (all but CCS and MR)
        self.technology_name = technology

        self.engine = engine
        self.time_to = time_to
        self.time_from = time_from
        self.project_id = project_id

        self.vectors = vectors

        # Power users
        if verbose: print('  - Fetching Panel A users...')
        self.users_panel_a = self.get_panel_users(min_n_cdr=4, max_n_cdr=10000, max_tpd=100)
        self.tweet_ids_panel_a = set([tid for u in self.users_panel_a for tid in u['tids']])

        # mid-range users
        if verbose: print('  - Fetching Panel B users...')
        self.users_panel_b = self.get_panel_users(min_n_cdr=2, max_n_cdr=4, max_tpd=100)
        self.tweet_ids_panel_b = set([tid for u in self.users_panel_b for tid in u['tids']])

        # one-timers
        if verbose: print('  - Fetching Panel C users...')
        self.users_panel_c = self.get_panel_users(min_n_cdr=1, max_n_cdr=2, max_tpd=100)
        self.tweet_ids_panel_c = set([tid for u in self.users_panel_c for tid in u['tids']])

        if verbose: self.print_info()

        # fetch the respective item_ids per bucket (aka time window)
        if verbose: print('  - Fetching item_ids...')
        self.bucketed_tweets_panel_a = self.get_bucketed_tweets(list(self.tweet_ids_panel_a))
        self.bucketed_tweets_panel_b = self.get_bucketed_tweets(list(self.tweet_ids_panel_b))
        self.bucketed_tweets_panel_c = self.get_bucketed_tweets(list(self.tweet_ids_panel_c))

        # list of all buckets
        self.buckets = [bucket['bucket'] for bucket in self.bucketed_tweets_panel_a]
        if verbose: print(f'  - Loaded data across {len(self.buckets)} buckets (aka time windows)...')

        # fetch labels from item_ids, calculate spread (std) of vectors in each panel/bucket,
        #   and interpolate centroids for panel/buckets that had no tweets
        if verbose: print('  - Building centroids for panel A...')
        self.vectors_panel_a = self.get_vectors(self.bucketed_tweets_panel_a)
        self.deviations_panel_a = [np.std(bucket) if bucket is not None else 0 for bucket in self.vectors_panel_a]
        self.centroids_panel_a = self.get_centroids(self.vectors_panel_a)

        if verbose: print('  - Building centroids for panel B...')
        self.vectors_panel_b = self.get_vectors(self.bucketed_tweets_panel_b)
        self.deviations_panel_b = [np.std(bucket) if bucket is not None else 0 for bucket in self.vectors_panel_b]
        self.centroids_panel_b = self.get_centroids(self.vectors_panel_b)

        if verbose: print('  - Building centroids for panel C...')
        self.vectors_panel_c = self.get_vectors(self.bucketed_tweets_panel_c)
        self.deviations_panel_c = [np.std(bucket) if bucket is not None else 0 for bucket in self.vectors_panel_c]
        self.centroids_panel_c = self.get_centroids(self.vectors_panel_c)

    def print_info(self):
        print(f'=> {self.technology_name} ({self.technology})')
        print(f'   Panel A: {len(self.users_panel_a):,} users with '
              f'{sum([u["n_cdr_tweets"] for u in self.users_panel_a]):,} CDR tweets')
        print(f'   Panel B: {len(self.users_panel_b):,} users with '
              f'{sum([u["n_cdr_tweets"] for u in self.users_panel_b]):,} CDR tweets')
        print(f'   Panel C: {len(self.users_panel_c):,} users with '
              f'{sum([u["n_cdr_tweets"] for u in self.users_panel_c]):,} CDR tweets')

    def get_panel_users(self, min_n_cdr, max_n_cdr, max_tpd):
        tech_filter = '= :tech' if self.technology is not None else '> 1'
        stmt = text(f"""
        WITH tmp AS (
            SELECT twitter_id, ti.twitter_author_id,
                  (ti."user" -> 'username')::text                                                     as username,
                  (ti."user" -> 'created_at')::text::timestamp                                        as created,
                  extract('day' from date_trunc('day', :time_to ::timestamp -
                                                       (ti."user" -> 'created_at')::text::timestamp)) as days,
                  (ti."user" -> 'tweet_count')::int                                                   as n_tweets
            FROM twitter_item ti
            LEFT JOIN bot_annotation ba_tech ON (
                                ti.item_id = ba_tech.item_id
                            AND ba_tech.bot_annotation_metadata_id = 'fc73da56-9f51-4d2b-ad35-2a01dbe9b275'
                            AND ba_tech.key = 'tech')
            WHERE ti.project_id = :project_id
              AND ti.created_at >= :time_from ::timestamp
              AND ti.created_at <= :time_to ::timestamp
              AND ba_tech.value_int {tech_filter}),
            users AS (
            SELECT twitter_author_id, 
                   MAX(username) as username,
                   MAX(created) as created, 
                   MAX(days) as days, 
                   MAX(n_tweets) as n_tweets,
                   COUNT(DISTINCT twitter_id)      as n_cdr_tweets,
                   array_agg(DISTINCT twitter_id)  AS tids
            FROM tmp
            GROUP BY twitter_author_id
            )
        SELECT * FROM users
        WHERE n_tweets/days  <= :max_tpd
          AND n_cdr_tweets >= :min_n_cdr
          AND n_cdr_tweets < :max_n_cdr
        """)
        with self.engine.session() as session:
            res = session.execute(stmt, {
                'project_id': self.project_id,
                'time_from': self.time_from,
                'time_to': self.time_to,
                'max_tpd': max_tpd,
                'max_n_cdr': max_n_cdr,
                'min_n_cdr': min_n_cdr,
                'tech': self.technology
            }).mappings().all()
            return [dict(row) for row in res]

    def get_bucketed_tweets(self, tids: list[str]):
        with self.engine.session() as session:
            stmt = text("""
            WITH buckets as (SELECT generate_series(:time_from ::timestamp, :time_to ::timestamp, :window) as bucket),
                 twts as (SELECT ti.item_id, ti.created_at, ti.twitter_id 
                          FROM twitter_item ti 
                          WHERE ti.project_id = :project_id AND ti.twitter_id = ANY(:tids))
            SELECT b.bucket, count(tw.twitter_id) cnt,
                   array_agg(tw.twitter_id) tids,
                   array_agg(tw.created_at) timestamps,
                   array_agg(tw.item_id) item_ids
            FROM buckets b LEFT OUTER JOIN
            twts tw ON (tw.created_at >= (b.bucket - :window ::interval) AND tw.created_at < b.bucket)
            GROUP BY b.bucket
            ORDER BY b.bucket
            """)

            res = session.execute(stmt, {
                'project_id': self.project_id,
                'time_from': self.time_from,
                'time_to': self.time_to,
                'tids': tids,
                'window': '1 week'
            }).mappings().all()
            return [dict(row) for row in res]

    def get_vectors(self, bucketed_tweets) -> list[list[np.ndarray] | None]:
        return [
            np.array([list(self.vectors.get_xy(item_id)) for item_id in bucket['item_ids']])
            if bucket['cnt'] > 0 else None
            for bucket in bucketed_tweets
        ]

    def get_centroids(self, vectors: list[list[np.ndarray] | None]) -> np.ndarray:
        # collapse vectors to mean (or nan)
        centroids = np.array([
            np.array(bucket).mean(axis=0)
            if bucket is not None else np.array([np.nan, np.nan])
            for bucket in vectors
        ])

        def fill_nans(col):
            nans = np.isnan(col)
            nz = lambda z: z.nonzero()[0]
            col[nans] = np.interp(nz(nans), nz(~nans), col[~nans])
            return col

        # fill nans / interpolate
        centroids[:, 0] = fill_nans(centroids[:, 0])
        centroids[:, 1] = fill_nans(centroids[:, 1])
        return centroids


class Containers:
    def __init__(self, vectors: Vectors, engine: DatabaseEngine,
                 time_from: datetime.datetime, time_to: datetime.datetime, project_id: str, verbose=True):
        self.containers = {
            q: Container(technology_int=i, technology=q, vectors=vectors, engine=engine,
                         time_from=time_from, time_to=time_to, project_id=project_id, verbose=verbose)
            for i, q in enumerate(queries.keys())
            if i > 1
        }
        self.containers['All'] = Container(technology_int=None, technology='All', vectors=vectors, engine=engine,
                                           time_from=time_from, time_to=time_to, project_id=project_id, verbose=verbose)

        if verbose:
            print('Building lookup index...')
        self.lookup = {i: q for i, q in enumerate(queries.keys()) if i > 1}
        self.lookup[-1] = 'All'

    @property
    def keys(self) -> list[str]:
        return list(self.containers.keys())

    @property
    def technologies(self) -> list[str]:
        ret = list(self.containers.keys())
        ret.remove('All')
        return ret

    def tc(self, name: str) -> Container:
        return self.containers[name]

    def ic(self, i: int) -> Container:
        return self.containers[self.lookup[i]]


def main():
    TSNE_FILE = 'data/geo/vectors/vec_2d_tsne_mean_10_all'

    TIME_FROM = datetime.datetime(2010, 1, 1, 0, 0, 0)
    TIME_TO = datetime.datetime(2022, 12, 31, 23, 59, 59)

    PROJECT_ID = 'c5d36b2e-cbb4-47a8-8370-e5f52bb78bf3'

    ENGINE: DatabaseEngine = get_engine(settings=settings)

    vectors = Vectors(TSNE_FILE)


if __name__ == '__main__':
    pass
