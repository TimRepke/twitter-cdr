import logging
from enum import Enum
from pathlib import Path

import typer
import hnswlib
import numpy as np

from common.config import settings
from common.pyw_hnsw import Index, DuplicateFreeIndex
from common.vector_index import VectorIndex


class Algorithm(str, Enum):
    tsne = 'tsne'
    umap = 'umap'


def main(embeddings_file: str | None = None,
         target_file: str | None = None,
         algo: Algorithm = Algorithm.tsne,
         sim_metric: str = 'cosine',  # for dimensionality reduction
         target_dims: int = 2,

         tsne_dof: float = 0.8,
         tsne_perplexity: int = 500,
         tsne_n_iter: int = 500,
         tsne_k: int = 1500,  # by default in openTSNE 3 * perplexity
         tsne_nn_batch_size: int = 2000,

         seed: int = 43,

         space: str = 'cosine',  # as used by hnsw index
         source_dims: int = 384,
         df_index: bool = True,  # using duplicate free index if True
         tsne_verbose: bool = True,
         log_level: str = 'DEBUG',
         default_log_level: str = 'WARNING'
         ):
    logging.basicConfig(format='%(asctime)s [%(levelname)s] %(name)s: %(message)s', level=default_log_level)
    logger = logging.getLogger('reduce')
    logger.setLevel(log_level)

    if embeddings_file is None:
        if df_index:
            embeddings_file = Path(settings.DATA_VECTORS) / 'embeddings_df'
        else:
            embeddings_file = Path(settings.DATA_VECTORS) / 'embeddings'
    else:
        embeddings_file = Path(embeddings_file)
    logger.debug(f'Reading embeddings from: {embeddings_file}')

    if target_file is None:
        target_file = Path(settings.DATA_VECTORS) / f'vec_{target_dims}d_{algo.value}'
    else:
        target_file = Path(target_file)
    logger.debug(f'Writing to: {target_file}')

    logger.info(f'Loading hnswlib index')
    if df_index:
        index = DuplicateFreeIndex(space=space, dim=source_dims)
    else:
        index = Index(space=space, dim=source_dims)
    index.load_index(embeddings_file)
    logger.debug(f' ... loaded {len(index.dict_labels)} vectors.')

    logger.debug('Fetching embeddings from index...')
    labels, embeddings = index.get_all_items()

    if algo == Algorithm.tsne:
        from openTSNE import TSNE
        from openTSNE.affinity import PerplexityBasedNN
        from openTSNE.nearest_neighbors import PrecomputedNeighbors
        from openTSNE.initialization import pca

        logger.info(f'Going to reduce dimensions to {target_dims} using tSNE')

        logger.debug('Downsampling!')
        ds_labels = [l[0] for i, l in enumerate(labels) if i % 2 == 0]
        ds_embeddings = np.array([v for i, v in enumerate(embeddings) if i % 2 == 0])
        logger.debug(f' --> ended up with {len(ds_labels)} labels and {ds_embeddings.shape} embeddings')

        del labels
        del embeddings
        del index

        logger.info('Making new index')
        hwind = hnswlib.Index(space='cosine', dim=source_dims)
        hwind.init_index(max_elements=len(ds_labels),
                         random_seed=seed, ef_construction=200, M=64)
        hwind.add_items(ds_embeddings)

        logger.info('Computing nearest neighbours...')
        # Set ef parameter for (ideal) precision/recall
        hwind.set_ef(min(2 * tsne_k, len(ds_labels)))
        indices_batched = []
        distances_batched = []
        for batch_start in range(0, ds_embeddings.shape[0], tsne_nn_batch_size):
            logger.debug(f'  > Querying batch of items from {batch_start:,} to {batch_start + tsne_nn_batch_size:,}...')
            b_ids, b_dists = hwind.knn_query(ds_embeddings[batch_start:batch_start + tsne_nn_batch_size],
                                                   k=tsne_k + 1, num_threads=-2)
            indices_batched.append(b_ids)
            distances_batched.append(b_dists)

        logger.debug('Stacking indices and distances...')
        indices = np.vstack(indices_batched)
        distances = np.vstack(distances_batched)

        logger.info('Preparing precomputed neighbours index...')
        ot_index = PrecomputedNeighbors(neighbors=indices[:, 1:], distances=distances[:, 1:])

        logger.info('Computing affinities...')
        affinities = PerplexityBasedNN(
            # data=None,
            perplexity=tsne_perplexity,
            n_jobs=-2,  # -2 -> all but one core
            metric=sim_metric,
            random_state=seed,
            verbose=tsne_verbose,
            knn_index=ot_index
        )

        logger.info('Computing initialisation with PCA...')
        init = pca(ds_embeddings,
                   n_components=target_dims,
                   verbose=tsne_verbose,
                   random_state=seed)

        logger.info('Fine-tuning with tSNE...')
        projection = TSNE(
            n_components=target_dims,
            n_iter=tsne_n_iter,
            dof=tsne_dof,
            metric=sim_metric,
            n_jobs=-2,  # -2 -> all but one core
            verbose=tsne_verbose,
            random_state=seed
        ).fit(affinities=affinities, initialization=init)

        logger.debug('Done with tSNE!')
    else:  # if algo == Algorithm.umap:
        raise NotImplementedError

    logger.info('Adding data to vector index...')
    vi = VectorIndex()
    vi.add_items(projection, ds_labels)
    # vi.dict_labels = index.dict_labels

    logger.debug('Writing vector index to disk...')
    vi.save(target_file)


if __name__ == "__main__":
    typer.run(main)
