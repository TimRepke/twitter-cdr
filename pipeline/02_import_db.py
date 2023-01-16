import logging
import uuid
from collections import defaultdict
from pathlib import Path

from nacsos_data.db import DatabaseEngine
from nacsos_data.db.schemas import TwitterItem
from nacsos_data.db.schemas.annotations import AnnotationScheme
from nacsos_data.models.annotations import AnnotationSchemeLabel, AnnotationSchemeLabelChoice
from nacsos_data.db.schemas.bot_annotations import BotAnnotationMetaData, BotAnnotation
from nacsos_data.db.schemas.imports import Import, M2MImportItemType, M2MImportItem

from nacsos_data.models.items.twitter import TwitterItemModel
from sqlalchemy.orm import Session

from common.queries import queries
from common.config import settings

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s [%(levelname)s] %(name)s: %(message)s', level=logging.WARNING)
    logger = logging.getLogger('import')
    logger.setLevel('DEBUG')

    TARGET_DIR = Path(settings.DATA_RAW_TWEETS).resolve()
    logger.info(f'Reading data from {TARGET_DIR}')

    db_engine = DatabaseEngine(host=settings.HOST, port=settings.PORT,
                               user=settings.USER, password=settings.PASSWORD,
                               database=settings.DATABASE)
    logger.info(f'Connecting to database {db_engine._connection_str}')

    quid2cati = {
        query['qid']: cat_i
        for cat_i, (cat, sub_queries) in enumerate(queries.items())
        for q_i, query in enumerate(sub_queries)
    }
    quid2qi = {
        query['qid']: q_i
        for cat_i, (_, sub_queries) in enumerate(queries.items())
        for q_i, query in enumerate(sub_queries)
    }

    # keep track of queries for each twitter id
    twitter2nacsos_id: dict[str, str] = {}
    annotations = defaultdict(list[str])

    with db_engine.session() as session:  # type: Session
        for cat_i, (cat, sub_queries) in enumerate(queries.items()):
            logger.info(f'Uploading data for {cat} with {len(sub_queries)} sub-queries.')
            for q_i, entry in enumerate(sub_queries):
                logger.info(f'Uploading tweets for {cat} {entry["qid"]}')
                import_id = str(uuid.uuid4())
                import_orm = Import(import_id=import_id,
                                    project_id=settings.PROJECT_ID,
                                    type='script',
                                    user_id=settings.USER_ID,
                                    name=f'{cat} ({entry["qid"]})',
                                    description=f'Subquery *{entry["qid"]}* for *{cat}* '
                                                f'with query `{entry["query"]} -is:retweet lang:en`')

                session.add(import_orm)
                session.commit()

                for file, itype in [
                    ((TARGET_DIR / f'{entry["qid"]}_explicit.jsonl').resolve(), M2MImportItemType.explicit),
                    # ((TARGET_DIR / f'{entry["qid"]}_implicit.jsonl').resolve(), M2MImportItemType.implicit)
                ]:
                    logger.info(f'Preparing import of tweets from file {file}')

                    batch_tweets = []
                    batch_m2m = []
                    existing = 0
                    with open(file, 'r') as f_in:
                        for line in f_in:
                            tweet_raw = TwitterItemModel.parse_raw(line)

                            if tweet_raw.twitter_id in twitter2nacsos_id:
                                nacsos_tweet_id = twitter2nacsos_id[tweet_raw.twitter_id]
                                existing +=1
                            else:
                                nacsos_tweet_id = str(uuid.uuid4())
                                twitter2nacsos_id[tweet_raw.twitter_id] = nacsos_tweet_id

                                orm_tweet = TwitterItem(**tweet_raw.dict())
                                orm_tweet.item_id = nacsos_tweet_id
                                orm_tweet.project_id = settings.PROJECT_ID

                                batch_tweets.append(orm_tweet)
                                # session.add(orm_tweet)
                                # session.commit()

                            orm_m2m_i2i = M2MImportItem(item_id=nacsos_tweet_id,
                                                        import_id=import_id,
                                                        type='explicit')
                            batch_m2m.append(orm_m2m_i2i)
                            # session.add(orm_m2m_i2i)
                            # session.commit()

                            annotations[tweet_raw.twitter_id].append(entry['qid'])

                    logger.info(f'Gathered {len(batch_tweets)} tweets for insertion '
                                f'and skipping {existing} already existing tweets.')
                    logger.info('Bulk-inserting tweets...')
                    session.add_all(batch_tweets)
                    session.flush()
                    session.commit()

                    logger.info('Bulk-inserting m2m relations...')
                    session.add_all(batch_m2m)
                    session.flush()
                    session.commit()

                    logger.info('Done with this query.')



        # Create annotation scheme to annotate categories
        labels = [
            AnnotationSchemeLabel(
                name='Technology',
                key='tech',
                hint=None,
                max_repeat=40,
                required=True,
                kind='single',
                choices=[
                    AnnotationSchemeLabelChoice(
                        name=cat,
                        value=cat_i,
                        children=[
                            AnnotationSchemeLabel(
                                name=f'Subquery for "{cat}"',
                                key=f'sub_{cat_i}',
                                max_repeat=40,
                                required=True,
                                kind='single',
                                choices=[
                                    AnnotationSchemeLabelChoice(
                                        name=query['qid'],
                                        hint=query['query'],
                                        value=q_i
                                    ).dict()
                                    for q_i, query in enumerate(sub_queries)
                                ]
                            ).dict()
                        ]
                    ).dict()
                    for cat_i, (cat, sub_queries) in enumerate(queries.items())
                ]
            ).dict()
        ]

        scheme_id = str(uuid.uuid4())
        logger.info(f'Creating annotation scheme with id: {scheme_id}')
        scheme = AnnotationScheme(annotation_scheme_id=scheme_id,
                                  project_id=settings.PROJECT_ID,
                                  name='CDR Technologies',
                                  description='CDR technologies (annotated by respective queries)',
                                  labels=labels)
        session.add(scheme)
        session.commit()

        meta_id = str(uuid.uuid4())
        logger.info(f'Creating metadata item for bot annotations with id {meta_id}')
        meta = BotAnnotationMetaData(
            bot_annotation_metadata_id=meta_id,
            name='Query annotations',
            kind='SCRIPT',
            project_id=settings.PROJECT_ID,
            annotation_scheme_id=scheme_id
        )
        session.add(meta)
        session.commit()

        batch_parents = []
        batch_children = []

        logger.info(f'Adding annotations for {len(annotations)} tweets')
        for tweet_id, sub_queries in annotations.items():
            for repeat, sub_query in enumerate(sub_queries, start=1):
                cat_i = quid2cati[sub_query]
                q_i = quid2qi[sub_query]

                parent_id = str(uuid.uuid4())
                anno = BotAnnotation(
                    bot_annotation_id=parent_id,
                    bot_annotation_metadata_id=meta_id,
                    item_id=twitter2nacsos_id[tweet_id],
                    parent=None,
                    key='tech',
                    repeat=repeat,
                    value_int=cat_i
                )
                # session.add(anno)
                # session.commit()
                batch_parents.append(anno)

                sub_anno = BotAnnotation(
                    bot_annotation_id=str(uuid.uuid4()),
                    bot_annotation_metadata_id=meta_id,
                    item_id=twitter2nacsos_id[tweet_id],
                    parent=parent_id,
                    key=f'cat_{cat_i}',
                    repeat=1,
                    value_int=q_i
                )
                # session.add(sub_anno)
                # session.commit()
                batch_children.append(sub_anno)

        session.add_all(batch_parents)
        session.flush()
        session.commit()

        session.add_all(batch_children)
        session.flush()
        session.commit()

# DELETE
# FROM m2m_import_item
#     USING import
# WHERE import.project_id = 'c5d36b2e-cbb4-47a8-8370-e5f52bb78bf3'
#   AND import.import_id = m2m_import_item.import_id;
#
# DELETE
# FROM import
# WHERE import.project_id = 'c5d36b2e-cbb4-47a8-8370-e5f52bb78bf3';
#
# DELETE
# FROM twitter_item as it
#     USING item
# WHERE item.project_id = 'c5d36b2e-cbb4-47a8-8370-e5f52bb78bf3';
#
# DELETE
# FROM item
# WHERE item.project_id = 'c5d36b2e-cbb4-47a8-8370-e5f52bb78bf3';
#
# DELETE
# FROM bot_annotation
#     USING bot_annotation_metadata bam
# WHERE bam.project_id = 'c5d36b2e-cbb4-47a8-8370-e5f52bb78bf3'
#   AND bam.bot_annotation_metadata_id = bot_annotation.bot_annotation_metadata_id;
#
# DELETE
# FROM bot_annotation_metadata
# WHERE project_id = 'c5d36b2e-cbb4-47a8-8370-e5f52bb78bf3';
#
# DELETE
# FROM annotation_scheme
# WHERE project_id = 'c5d36b2e-cbb4-47a8-8370-e5f52bb78bf3';
