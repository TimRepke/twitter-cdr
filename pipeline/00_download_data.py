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
            logging.info(f'Getting tweets for {cat} {entry["qid"]} -> {entry["query"]}')

            file_explicit = (TARGET_DIR / f'{entry["qid"]}_explicit.jsonl').resolve()
            file_explicit.parent.mkdir(parents=True, exist_ok=True)
            file_conv_ids = (TARGET_DIR / f'{entry["qid"]}_conversations.txt').resolve()

            with open(file_explicit, 'w') as f_out_ex, \
                    open(file_conv_ids, 'w') as f_out_conv_ids:
                for tweet in download_query(entry['query'] + ' -is:retweet lang:en'):
                    f_out_ex.write(tweet.json() + '\n')
                    if tweet.conversation_id:
                        f_out_conv_ids.write(f'{tweet.conversation_id}\n')
