with users as (SELECT (ti."user" -> 'created_at')::text::timestamp                                        as created,
                      (ti."user" -> 'tweet_count')::int                                                   as n_tweets,
                      (ti."user" -> 'username')::text                                                     as username,
                      ti.twitter_author_id,
                      count(1)                                                                            as n_cdr_tweets,
                      extract('day' from date_trunc('day', '2022-12-31'::date -
                                                           (ti."user" -> 'created_at')::text::timestamp)) as days
               FROM twitter_item ti
               WHERE ti.project_id = 'c5d36b2e-cbb4-47a8-8370-e5f52bb78bf3'
                 AND ti.created_at >= '2010-01-01 00:00'::timestamp
                 AND ti.created_at <= '2022-12-31 23:59'::timestamp
               GROUP BY created, n_tweets, days, twitter_author_id, username)
SELECT username,
       twitter_author_id,
       created,
       n_tweets,
       n_cdr_tweets,
       days                as days_active,
       n_tweets / days     as tpd,
       n_cdr_tweets / days as tpd_cdr
FROM users
ORDER BY created DESC
OFFSET 50 LIMIT 10;


SELECT DISTINCT on (ti.twitter_author_id) ti.twitter_author_id,
                                          (ti."user" -> 'username')::text                                                     as username,
                                          (ti."user" -> 'created_at')::text::timestamp                                        as created,
                                          extract('day' from date_trunc('day', '2022-12-31'::date -
                                                                               (ti."user" -> 'created_at')::text::timestamp)) as days,
                                          (ti."user" -> 'tweet_count')::int                                                   as n_tweets,
                                          array_agg(twitter_id) OVER (PARTITION BY twitter_author_id)                         as tids,
                                          count(1) OVER (PARTITION BY twitter_author_id)                                      as n_cdr_tweets
FROM twitter_item ti
WHERE ti.project_id = 'c5d36b2e-cbb4-47a8-8370-e5f52bb78bf3'
  AND ti.created_at >= '2010-01-01 00:00'::timestamp
  AND ti.created_at <= '2022-12-31 23:59'::timestamp
LIMIT 10;



WITH tmp AS (SELECT ti.item_id,
                    twitter_id,
                    ti.twitter_author_id,
                    (ti."user" -> 'created_at')::text::timestamp                                        as created,
                    extract('day' from date_trunc('day', '2022-12-31 23:59'::timestamp -
                                                         (ti."user" -> 'created_at')::text::timestamp)) as days,
                    (ti."user" -> 'tweet_count')::int                                                   as n_tweets
             FROM twitter_item ti
                      LEFT JOIN bot_annotation ba_tech ON (
                         ti.item_id = ba_tech.item_id
                     AND ba_tech.bot_annotation_metadata_id = 'fc73da56-9f51-4d2b-ad35-2a01dbe9b275'
                     AND ba_tech.key = 'tech')
             WHERE ti.project_id = 'c5d36b2e-cbb4-47a8-8370-e5f52bb78bf3'
               AND ti.created_at >= '2010-01-01 00:00'::timestamp
               AND ti.created_at <= '2022-12-31 23:59'::timestamp
               AND ba_tech.value_int > 1),
     users AS (SELECT twitter_author_id,
                      days,
                      n_tweets,
                      COUNT(DISTINCT twitter_id)                                           as n_cdr_tweets,
                      count(DISTINCT tmp.twitter_id) FILTER (WHERE ba_senti.value_int = 2) as "Positive",
                      count(DISTINCT tmp.twitter_id) FILTER (WHERE ba_senti.value_int = 1) as "Neutral",
                      count(DISTINCT tmp.twitter_id) FILTER (WHERE ba_senti.value_int = 0) as "Negative"
               FROM tmp
                        LEFT JOIN bot_annotation ba_senti ON (
                           tmp.item_id = ba_senti.item_id
                       AND ba_senti.bot_annotation_metadata_id = 'e63da0c9-9bb5-4026-ab5e-7d5845cdc111'
                       AND ba_senti.key = 'senti'
                       AND ba_senti.repeat = 1)
               GROUP BY twitter_author_id, days, n_tweets),
     users_paneled AS (SELECT users.*,
                              CASE
                                  WHEN n_tweets / days <= 100
                                      AND n_cdr_tweets > 3 THEN 'A'
                                  WHEN n_tweets / days <= 100
                                      AND n_cdr_tweets >= 2
                                      AND n_cdr_tweets <= 3 THEN 'B'
                                  WHEN n_tweets / days <= 100
                                      AND n_cdr_tweets = 1 THEN 'C'
                                  ELSE 'EX'
                                  END as panel
                       FROM users)
SELECT panel, count(distinct twitter_author_id) as n_users, SUM(n_cdr_tweets) as n_tweets, SUM("Positive") as pos, SUM("Neutral") as neu, SUM("Negative") as neg
FROM users_paneled uf
GROUP BY panel
ORDER BY panel;
