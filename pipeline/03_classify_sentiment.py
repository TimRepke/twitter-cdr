import logging
import uuid
from pathlib import Path

import numpy as np
from sqlalchemy import text
from sqlalchemy.orm import Session

from nacsos_data.db import DatabaseEngine
from nacsos_data.db.schemas import TwitterItem
from nacsos_data.db.schemas.annotations import AnnotationScheme
from nacsos_data.models.annotations import AnnotationSchemeLabel, AnnotationSchemeLabelChoice
from nacsos_data.db.schemas.bot_annotations import BotAnnotationMetaData, BotAnnotation
from nacsos_data.db.schemas.imports import Import, M2MImportItemType, M2MImportItem
from nacsos_data.models.items.twitter import TwitterItemModel

from common.models import Classifier
from common.config import settings

BATCH_SIZE = 500
MODEL = 'cardiffnlp/twitter-roberta-base-sentiment-latest'
MODEL_PATH = Path(f'{settings.DATA_MODELS}') / 'cardiff_latest'

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s [%(levelname)s] %(name)s: %(message)s', level=logging.WARNING)
    logger = logging.getLogger('import')
    logger.setLevel('DEBUG')

    db_engine = DatabaseEngine(host=settings.HOST, port=settings.PORT,
                               user=settings.USER, password=settings.PASSWORD,
                               database=settings.DATABASE)
    logger.info(f'Connecting to database {db_engine._connection_str}')

    logger.info(f'Loading model "{MODEL}" (caching at {MODEL_PATH})')
    classifier = Classifier(hf_name=MODEL,
                            cache_dir=MODEL_PATH)
    classifier.load()

    with db_engine.session() as session:  # type: Session
        NUM_TWEETS = session.execute(text("SELECT count(1) "
                                          "FROM item "
                                          "WHERE project_id = :project_id;"),
                                     {'project_id': settings.PROJECT_ID}).scalar()
        logger.info(f'Found {NUM_TWEETS} to classify, going to process them in batches of {BATCH_SIZE}')

        label_map = {
            'negative': 0,
            'neutral': 1,
            'positive': 2
        }

        scheme_id = str(uuid.uuid4())
        logger.info(f'Creating annotation scheme with id: {scheme_id}')
        scheme = AnnotationScheme(annotation_scheme_id=scheme_id,
                                  project_id=settings.PROJECT_ID,
                                  name='Sentiment and Emotions',
                                  description='Sentiments and emotions',
                                  labels=[
                                      AnnotationSchemeLabel(
                                          name='Sentiment',
                                          key='senti',
                                          hint=None,
                                          max_repeat=3,
                                          required=True,
                                          kind='single',
                                          choices=[
                                              AnnotationSchemeLabelChoice(name=key, value=value).dict()
                                              for key, value in label_map.items()
                                          ]
                                      ).dict(),
                                      # FIXME add emotion label
                                  ])
        session.add(scheme)
        session.commit()

        meta_id = str(uuid.uuid4())
        logger.info(f'Creating metadata item for bot annotations with id {meta_id}')
        meta = BotAnnotationMetaData(
            bot_annotation_metadata_id=meta_id,
            name=f'Classification with {MODEL}',
            kind='SCRIPT',
            project_id=settings.PROJECT_ID,
            annotation_scheme_id=scheme_id
        )
        session.add(meta)
        session.commit()

        for batch_from in range(0, NUM_TWEETS, BATCH_SIZE):
            logger.info(f'Fetching batch with offset {batch_from}.')
            tweets = session.execute(text("SELECT item_id, text "
                                          "FROM item "
                                          "WHERE project_id = :project_id "
                                          "ORDER BY item_id "
                                          "OFFSET :batch_start LIMIT :batch_size;"),
                                     {
                                         'project_id': settings.PROJECT_ID,
                                         'batch_size': BATCH_SIZE,
                                         'batch_start': batch_from
                                     }).mappings().all()

            texts = classifier.preprocess([tweet['text'] for tweet in tweets])
            output = classifier.classify(texts, return_all_scores=True)

            annotations = [
                BotAnnotation(
                    bot_annotation_id=str(uuid.uuid4()),
                    bot_annotation_metadata_id=meta_id,
                    item_id=str(tweet['item_id']),
                    parent=None,
                    key='senti',
                    repeat=repeat,
                    value_int=label_map[label],
                    confidence=score
                )
                for tweet, res in zip(tweets, output)
                for repeat, (label, score) in enumerate(sorted(res.items(), key=lambda e: e[1], reverse=True), start=1)
            ]

            session.add_all(annotations)
            session.flush()
            session.commit()
