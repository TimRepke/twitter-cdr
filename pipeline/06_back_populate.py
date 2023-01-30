import logging
from enum import Enum
from pathlib import Path

import tqdm
import typer
import numpy as np

from common.config import settings
from common.pyw_hnsw import Index
from common.vector_index import VectorIndex


class Averaging(str, Enum):
    mean = 'mean'
    median = 'median'
    weighted = 'weighted'


# Possibly not all points could be used for dimensionality reduction before,
# either because of duplicates that had to be removed for the hnsw index or
# to reduce the amount of vectors. This function adds all previously missing
# elements to the projection.
def main(embeddings_file: str | None = None,
         reduced_file: str | None = None,
         target_file: str | None = None,
         averaging: Averaging = Averaging.mean,
         n_nearest: int = 10,
         space: str = 'cosine',  # as used by hnsw index
         source_dims: int = 384,
         log_level: str = 'DEBUG',
         default_log_level: str = 'WARNING'
         ):
    logging.basicConfig(format='%(asctime)s [%(levelname)s] %(name)s: %(message)s', level=default_log_level)
    logger = logging.getLogger('reduce')
    logger.setLevel(log_level)

    if embeddings_file is None:
        embeddings_file = Path(settings.DATA_VECTORS) / 'embeddings'
    else:
        embeddings_file = Path(embeddings_file)
    logger.debug(f'Reading embeddings from: {embeddings_file}')

    if reduced_file is None:
        reduced_file = Path(settings.DATA_VECTORS) / f'vec_2d_tsne'
    else:
        reduced_file = Path(reduced_file)
    logger.debug(f'Reading projection from: {reduced_file}')

    if target_file is None:
        target_file = Path(settings.DATA_VECTORS) / f'vec_2d_tsne_{averaging.value}_{n_nearest}_all'
    else:
        target_file = Path(target_file)
    logger.debug(f'Writing to: {target_file}')

    logger.info(f'Loading hnswlib index')
    index = Index(space=space, dim=source_dims)
    index.load_index(embeddings_file)
    index.set_ef(n_nearest * 10)
    logger.debug(f' ... loaded {len(index.dict_labels)} vectors.')
    logger.debug('Fetching embeddings from index...')
    labels, embeddings = index.get_all_items()

    logger.info('Loading projections...')
    p_index = VectorIndex()
    p_index.load(reduced_file)
    p_labels, p_vectors = p_index.get_all_items()
    id2idx = p_index.id2idx

    logger.info('Collecting projected vectors for all entries...')
    item_ids = labels
    vectors = []
    for i, iid in tqdm.tqdm(enumerate(item_ids)):
        if iid in id2idx:  # was already included in original projection
            vectors.append(p_vectors[id2idx[iid]])
        else:  # wasn't in the original projection, compute neighbourhood
            for knn_factor in range(2, 20, 1):  # try with repeatedly growing search radius
                neighbours, distances = index.knn_query(np.array(embeddings[i]), k=n_nearest * knn_factor)
                neighbour_ids = [
                    (v, d)
                    for v, d in zip(neighbours[0][1:], distances[0][1:])
                    if v in id2idx
                ]
                if len(neighbour_ids) >= n_nearest:
                    break
            else:  # reached max search radius, exit with exception
                raise RuntimeError('Couldn\' find enough neighbours!')

            neighbour_vectors = np.array([
                p_vectors[id2idx[ni[0]]]
                for ni in neighbour_ids[:n_nearest]
            ])

            if averaging == Averaging.mean:
                vectors.append(np.mean(neighbour_vectors, axis=0))
            elif averaging == Averaging.median:
                vectors.append(np.median(neighbour_vectors, axis=0))
            elif averaging == Averaging.weighted:
                vectors.append(np.average(neighbour_vectors, axis=0,
                                          weights=np.array([d for _, d in neighbour_ids[:n_nearest]])))

    logger.info('Adding data to vector index...')
    vi = VectorIndex()
    vi.add_items(np.array(vectors), item_ids)
    # vi.dict_labels = index.dict_labels

    logger.debug('Writing vector index to disk...')
    vi.save(target_file)


if __name__ == "__main__":
    typer.run(main)
