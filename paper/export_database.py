from shared.db import run_query
from sqlalchemy import text
import pandas as pd

stmt = text('''
    WITH sentiments AS (SELECT item_id,
                               json_agg(ba.value_int)  as sentiment,
                               json_agg(ba.confidence) as sentiment_score
                        FROM bot_annotation ba
                        WHERE ba.bot_annotation_metadata_id = 'e63da0c9-9bb5-4026-ab5e-7d5845cdc111'
                          AND ba.key = 'senti'
                        GROUP BY item_id),
         technologies AS (SELECT ba.item_id,
                                 json_agg(json_build_object(
                                         'tech', ba.value_int,
                                         'query', sub.value_int)) as technologies
                          FROM bot_annotation ba
                                   LEFT JOIN bot_annotation sub ON sub.parent = ba.bot_annotation_id
                          WHERE ba.bot_annotation_metadata_id = 'fc73da56-9f51-4d2b-ad35-2a01dbe9b275'
                            AND ba.key = 'tech'
                          GROUP BY ba.item_id)
    SELECT ti.twitter_id, ti.twitter_author_id, s.sentiment, s.sentiment_score, t.technologies
    FROM twitter_item ti
             LEFT JOIN sentiments s ON s.item_id = ti.item_id
             LEFT JOIN technologies t ON t.item_id = ti.item_id
    WHERE ti.project_id = 'c5d36b2e-cbb4-47a8-8370-e5f52bb78bf3'
    ORDER BY ti.created_at
''')
result = run_query(stmt, {})
df = pd.DataFrame(result)
df.to_csv('data/dataset.tsv', sep='\t', encoding='utf-8', index=False)
