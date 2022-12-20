from pathlib import Path
import logging
from common.queries import queries
from common.twitter import download_query
from common.config import settings

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s [%(levelname)s] %(name)s: %(message)s', level=logging.DEBUG)
    TARGET_DIR = Path(settings.DATA_RAW_TWEETS)
    for cat, sub_queries in queries.items():
        logging.info(f'Looking at {cat} with {len(sub_queries)} sub-queries.')
        for entry in sub_queries:
            logging.info(f'Getting threads for {cat} {entry["qid"]}')

            file_implicit = (TARGET_DIR / f'{entry["qid"]}_implicit.jsonl').resolve()
            file_conv_ids = (TARGET_DIR / f'{entry["qid"]}_conversations.txt').resolve()

            with open(file_conv_ids, 'r') as f_in_conv_ids:
                conv_ids = [conv_id for conv_id in f_in_conv_ids]

            logging.info(f'There are {len(conv_ids)} (unique: {len(set(conv_ids))}) conversations'
                         f' for {entry["qid"]} in {cat}')

            with open(file_implicit, 'w') as f_out_impl:
                for conv_id in set(conv_ids):
                    for tweet in download_query(f'conversation_id:{conv_id} -is:retweet lang:en'):
                        f_out_impl.write(tweet.json() + '\n')
