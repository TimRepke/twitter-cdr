import logging
from pathlib import Path

import hnswlib
import typer
from sqlalchemy import text
from sqlalchemy.orm import Session

from nacsos_data.db import DatabaseEngine

from common.models import Embedder
from common.config import settings
from common.pyw_hnsw import Index


def main(model: str = 'all-MiniLM-L6-v2',
         model_path: str | None = None,
         target_file: str | None = None,
         batch_size: int = 500,
         space: str = 'cosine',
         dims: int = 384,
         ef_const: int = 200,
         M_const: int = 16,
         seed: int = 43,
         log_level: str = 'DEBUG',
         default_log_level: str = 'WARNING'
         ):
    logging.basicConfig(format='%(asctime)s [%(levelname)s] %(name)s: %(message)s', level=default_log_level)
    logger = logging.getLogger('embed')
    logger.setLevel(log_level)

    if model_path is None:
        model_path = Path(settings.DATA_MODELS) / 'minilm_l6_v2'
    else:
        model_path = Path(model_path)
    logger.debug(f'Cache for embedding model: {model_path}')

    if target_file is None:
        target_file = Path(settings.DATA_VECTORS) / 'embeddings'
    else:
        target_file = Path(target_file)
    logger.debug(f'Target for output file: {target_file}')

    db_engine = DatabaseEngine(host=settings.HOST, port=settings.PORT,
                               user=settings.USER, password=settings.PASSWORD,
                               database=settings.DATABASE)
    logger.info(f'Connecting to database {db_engine._connection_str}')

    logger.info(f'Loading model "{model}" (caching at {model_path})')
    embedder = Embedder(hf_name=model, cache_dir=model_path)
    embedder.load()

    with db_engine.session() as session:  # type: Session
        NUM_TWEETS = session.execute(text("SELECT count(1) "
                                          "FROM item "
                                          "WHERE project_id = :project_id;"),
                                     {'project_id': settings.PROJECT_ID}).scalar()
        logger.info(f'Found {NUM_TWEETS} to embed, going to process them in batches of {batch_size}')

        logger.info(f'Preparing hnswlib index')

        index = Index(space=space, dim=dims)
        index.init_index(max_elements=NUM_TWEETS, ef_construction=ef_const, M=M_const, random_seed=seed)

        for batch_from in range(0, NUM_TWEETS, batch_size):
            logger.info(f'Fetching batch with offset {batch_from}.')
            tweets = session.execute(text("""
                                          SELECT item.item_id, item.text
                                          FROM item
                                              JOIN twitter_item ti on item.item_id = ti.item_id
                                          WHERE item.project_id = :project_id
                                          ORDER BY ti.created_at
                                          OFFSET :batch_start LIMIT :batch_size;
                                          """),
                                     {
                                         'project_id': settings.PROJECT_ID,
                                         'batch_size': batch_size,
                                         'batch_start': batch_from
                                     }).mappings().all()

            texts = embedder.preprocess([tweet['text'] for tweet in tweets])
            uuids = [str(tweet['item_id']) for tweet in tweets]
            embeddings = embedder.embed(texts)

            logger.debug(f'Appending {len(uuids)} items to index.')
            index.add_items(embeddings, uuids)

        logger.info('Saving index.')
        index.save_index(target_file)

if __name__ == "__main__":
    typer.run(main)
