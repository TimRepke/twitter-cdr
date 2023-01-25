import logging
from enum import Enum
from pathlib import Path

import typer

from common.config import settings
from common.pyw_hnsw import Index
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

         seed: int = 43,

         space: str = 'cosine',  # as used by hnsw index
         source_dims: int = 384,
         tsne_verbose: bool=True,
         log_level: str = 'DEBUG',
         default_log_level: str = 'WARNING'
         ):
    logging.basicConfig(format='%(asctime)s [%(levelname)s] %(name)s: %(message)s', level=default_log_level)
    logger = logging.getLogger('embed')
    logger.setLevel(log_level)

    if embeddings_file is None:
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
    index = Index(space=space, dim=source_dims)
    index.load_index(embeddings_file)
    logger.debug(f' ... loaded {len(index.dict_labels)} vectors.')

    logger.debug('Fetching embeddings from index...')
    labels, embeddings = index.get_all_items()

    if algo == Algorithm.tsne:
        from openTSNE import TSNE
        from openTSNE.affinity import PerplexityBasedNN
        from openTSNE.initialization import pca
        logger.info(f'Going to reduce dimensions to {target_dims} using tSNE')

        logger.debug('Computing affinities...')
        affinities = PerplexityBasedNN(
            embeddings,
            perplexity=tsne_perplexity,
            n_jobs=-2,  # -2 -> all but one core
            metric=sim_metric,
            random_state=seed,
            verbose=tsne_verbose
        )

        logger.debug('Computing initialisation with PCA...')
        init = pca(embeddings,
                   n_components=target_dims,
                   verbose=tsne_verbose,
                   random_state=seed)

        logger.debug('Fine-tuning with tSNE...')
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
    vi.add_items(projection, labels)

    logger.debug('Writing vector index to disk...')
    vi.save(target_file)


if __name__ == "__main__":
    typer.run(main)
