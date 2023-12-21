with users as (SELECT MAX((ti."user" -> 'created_at')::text::timestamp)                                        as created,
                      MAX((ti."user" -> 'tweet_count')::int)                                                   as n_tweets,
                      MAX((ti."user" -> 'username')::text)                                                     as username,
                      ti.twitter_author_id,
                      count(1)                                                                                 as n_cdr_tweets,
                      MAX(extract('day' from date_trunc('day', '2022-12-31'::date -
                                                               (ti."user" -> 'created_at')::text::timestamp))) as days
               FROM twitter_item ti
               WHERE ti.project_id = 'c5d36b2e-cbb4-47a8-8370-e5f52bb78bf3'
                 AND ti.created_at >= '2010-01-01 00:00'::timestamp
                 AND ti.created_at <= '2022-12-31 23:59'::timestamp
               GROUP BY twitter_author_id)
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
                      MAX(days)                                                            as days,
                      MAX(n_tweets)                                                        as n_tweets,
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
               GROUP BY twitter_author_id),
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
SELECT panel,
       count(distinct twitter_author_id) as n_users,
       SUM(n_cdr_tweets)                 as n_tweets,
       SUM("Positive")                   as pos,
       SUM("Neutral")                    as neu,
       SUM("Negative")                   as neg
FROM users_paneled uf
GROUP BY panel
ORDER BY panel;


-- Export panel data (all users with some stats and panel assignment)
WITH tmp AS (SELECT ti.item_id,
                    twitter_id,
                    ti.twitter_author_id,
                    ba_tech.value_int                                                                   as technology,
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
               AND ti.created_at >= '2006-01-01 00:00'::timestamp
               AND ti.created_at <= '2022-12-31 23:59'::timestamp
               AND ba_tech.value_int > 1),
     users AS (SELECT twitter_author_id,
                      MAX(days)                                                            as days,
                      MAX(n_tweets)                                                        as n_tweets,
                      COUNT(DISTINCT tmp.twitter_id)                                       as n_cdr_tweets,
                      COUNT(DISTINCT tmp.twitter_id) FILTER (WHERE technology > 1)         as n_cdr_tweets_noccs,
                      count(DISTINCT tmp.twitter_id) FILTER (WHERE ba_senti.value_int = 2) as "Positive",
                      count(DISTINCT tmp.twitter_id) FILTER (WHERE ba_senti.value_int = 1) as "Neutral",
                      count(DISTINCT tmp.twitter_id) FILTER (WHERE ba_senti.value_int = 0) as "Negative"
               FROM tmp
                        LEFT JOIN bot_annotation ba_senti ON (
                           tmp.item_id = ba_senti.item_id
                       AND ba_senti.bot_annotation_metadata_id = 'e63da0c9-9bb5-4026-ab5e-7d5845cdc111'
                       AND ba_senti.key = 'senti'
                       AND ba_senti.repeat = 1)
               GROUP BY twitter_author_id),
     users_paneled AS (SELECT users.*,
                              CASE
                                  WHEN n_tweets / days <= 100
                                      AND n_cdr_tweets_noccs < 3 THEN 'A'
                                  WHEN n_tweets / days <= 100
                                      AND n_cdr_tweets_noccs > 2
                                      AND n_cdr_tweets_noccs <= 50 THEN 'B'
                                  WHEN n_tweets / days <= 100
                                      AND n_cdr_tweets_noccs > 50 THEN 'C'
                                  ELSE 'EX'
                                  END as panel
                       FROM users)
SELECT panel,
       twitter_author_id,
       "Positive",
       "Neutral",
       "Negative",
       n_cdr_tweets,
       n_cdr_tweets_noccs,
       n_tweets,
       days,
       n_tweets / days as tpd
FROM users_paneled;

-- Tweet counts per technology and panel
WITH tweets AS (SELECT ti.item_id,
                       ti.twitter_id,
                       ti.created_at,
                       ti.twitter_author_id,
                       ba_tech.value_int                                                                   as technology,
                       (ti."user" -> 'created_at')::text::timestamp                                        as created,
                       extract('day' from date_trunc('day', :end_time ::timestamp -
                                                            (ti."user" -> 'created_at')::text::timestamp)) as days,
                       (ti."user" -> 'tweet_count')::int                                                   as n_tweets
                FROM twitter_item ti
                         LEFT JOIN bot_annotation ba_tech ON (
                            ti.item_id = ba_tech.item_id
                        AND ba_tech.bot_annotation_metadata_id = :ba_tech
                        AND ba_tech.key = 'tech')
                WHERE ti.project_id = :project_id
                  AND ti.created_at >= :start_time ::timestamp
                  AND ti.created_at <= :end_time ::timestamp),
     users_pre AS (SELECT twitter_author_id,
                          AVG(days)                                                as days,
                          AVG(n_tweets)                                            as n_tweets,
                          COUNT(DISTINCT twitter_id)                               as n_cdr_tweets,
                          COUNT(DISTINCT twitter_id) FILTER (WHERE technology > 1) as n_cdr_tweets_noccs
                   FROM tweets
                   GROUP BY twitter_author_id),
     users AS (SELECT users_pre.*,
                      CASE
                          WHEN n_tweets / days <= 100
                              AND n_cdr_tweets <= 2 THEN 'A'
                          WHEN n_tweets / days <= 100
                              AND n_cdr_tweets > 2
                              AND n_cdr_tweets <= 50 THEN 'B'
                          WHEN n_tweets / days <= 100
                              AND n_cdr_tweets > 50 THEN 'C'
                          ELSE 'EX'
                          END as panel
               FROM users_pre)
SELECT ti.technology                                                        as technology,
       count(DISTINCT ti.twitter_id) filter ( where u.panel = 'A' )         as tweets_a,
       count(DISTINCT ti.twitter_id) filter ( where u.panel = 'B' )         as tweets_b,
       count(DISTINCT ti.twitter_id) filter ( where u.panel = 'C' )         as tweets_c,
       count(DISTINCT ti.twitter_id) filter ( where u.panel = 'EX' )        as tweets_ex,
       count(DISTINCT ti.twitter_id)                                        as tweets_all,
       count(DISTINCT ti.twitter_author_id) filter ( where u.panel = 'A' )  as users_a,
       count(DISTINCT ti.twitter_author_id) filter ( where u.panel = 'B' )  as users_b,
       count(DISTINCT ti.twitter_author_id) filter ( where u.panel = 'C' )  as users_c,
       count(DISTINCT ti.twitter_author_id) filter ( where u.panel = 'EX' ) as users_ex,
       count(DISTINCT ti.twitter_author_id)                                 as users_all
FROM tweets ti
         LEFT OUTER JOIN users u ON ti.twitter_author_id = u.twitter_author_id
GROUP BY ti.technology
UNION
SELECT 100                                                                  as technology,
       count(DISTINCT ti.twitter_id) filter ( where u.panel = 'A' )         as tweets_a,
       count(DISTINCT ti.twitter_id) filter ( where u.panel = 'B' )         as tweets_b,
       count(DISTINCT ti.twitter_id) filter ( where u.panel = 'C' )         as tweets_c,
       count(DISTINCT ti.twitter_id) filter ( where u.panel = 'EX' )        as tweets_ex,
       count(DISTINCT ti.twitter_id)                                        as tweets_all,
       count(DISTINCT ti.twitter_author_id) filter ( where u.panel = 'A' )  as users_a,
       count(DISTINCT ti.twitter_author_id) filter ( where u.panel = 'B' )  as users_b,
       count(DISTINCT ti.twitter_author_id) filter ( where u.panel = 'C' )  as users_c,
       count(DISTINCT ti.twitter_author_id) filter ( where u.panel = 'EX' ) as users_ex,
       count(DISTINCT ti.twitter_author_id)                                 as users_all
FROM tweets ti
         LEFT OUTER JOIN users u ON ti.twitter_author_id = u.twitter_author_id
WHERE ti.technology > 1
UNION
SELECT 200                                                                  as technology,
       count(DISTINCT ti.twitter_id) filter ( where u.panel = 'A' )         as tweets_a,
       count(DISTINCT ti.twitter_id) filter ( where u.panel = 'B' )         as tweets_b,
       count(DISTINCT ti.twitter_id) filter ( where u.panel = 'C' )         as tweets_c,
       count(DISTINCT ti.twitter_id) filter ( where u.panel = 'EX' )        as tweets_ex,
       count(DISTINCT ti.twitter_id)                                        as tweets_all,
       count(DISTINCT ti.twitter_author_id) filter ( where u.panel = 'A' )  as users_a,
       count(DISTINCT ti.twitter_author_id) filter ( where u.panel = 'B' )  as users_b,
       count(DISTINCT ti.twitter_author_id) filter ( where u.panel = 'C' )  as users_c,
       count(DISTINCT ti.twitter_author_id) filter ( where u.panel = 'EX' ) as users_ex,
       count(DISTINCT ti.twitter_author_id)                                 as users_all
FROM tweets ti
         LEFT OUTER JOIN users u ON ti.twitter_author_id = u.twitter_author_id
ORDER BY 1;


-- Tweet counts per sentiment per technology and panel
WITH tweets AS (SELECT ti.item_id,
                       ti.twitter_id,
                       ti.created_at,
                       ti.twitter_author_id,
                       ba_tech.value_int                                                                   as technology,
                       ba_senti.value_int                                                                  as sentiment,
                       (ti."user" -> 'created_at')::text::timestamp                                        as created,
                       extract('day' from date_trunc('day', '2023-01-01'::timestamp -
                                                            (ti."user" -> 'created_at')::text::timestamp)) as days,
                       (ti."user" -> 'tweet_count')::int                                                   as n_tweets
                FROM twitter_item ti
                         LEFT JOIN bot_annotation ba_tech ON (
                            ti.item_id = ba_tech.item_id
                        AND ba_tech.bot_annotation_metadata_id = 'fc73da56-9f51-4d2b-ad35-2a01dbe9b275'
                        AND ba_tech.key = 'tech')
                         LEFT JOIN bot_annotation ba_senti ON (
                            ti.item_id = ba_senti.item_id
                        AND ba_senti.bot_annotation_metadata_id = 'e63da0c9-9bb5-4026-ab5e-7d5845cdc111'
                        AND ba_senti.key = 'senti'
                        AND ba_senti.repeat = 1)
                WHERE ti.project_id = 'c5d36b2e-cbb4-47a8-8370-e5f52bb78bf3'
                  AND ti.created_at >= '2006-01-01'::timestamp
                  AND ti.created_at <= '2023-01-01'::timestamp
                  AND ba_tech.value_int > 1),
     users_pre AS (SELECT twitter_author_id,
                          AVG(days)                                                as days,
                          AVG(n_tweets)                                            as n_tweets,
                          COUNT(DISTINCT twitter_id)                               as n_cdr_tweets,
                          COUNT(DISTINCT twitter_id) FILTER (WHERE technology > 1) as n_cdr_tweets_noccs
                   FROM tweets
                   GROUP BY twitter_author_id),
     users AS (SELECT users_pre.*,
                      CASE
                          WHEN n_tweets / days <= 100
                              AND n_cdr_tweets <= 2 THEN 'A'
                          WHEN n_tweets / days <= 100
                              AND n_cdr_tweets > 2
                              AND n_cdr_tweets <= 50 THEN 'B'
                          WHEN n_tweets / days <= 100
                              AND n_cdr_tweets > 50 THEN 'C'
                          ELSE 'EX'
                          END as panel
               FROM users_pre),
     dat AS (SELECT ti.created_at as created_at,
                    ti.twitter_id as twitter_id,
                    ti.sentiment  as sentiment,
                    ti.technology as technology,
                    u.panel       as panel
             FROM tweets ti
                      LEFT OUTER JOIN users u ON ti.twitter_author_id = u.twitter_author_id)
SELECT d.technology                                                                    as technology
     , count(DISTINCT d.twitter_id)                                                    as tweets_all
     , count(DISTINCT d.twitter_id) filter ( where d.sentiment = 2 )                   as tweets_all_pos
     , count(DISTINCT d.twitter_id) filter ( where d.sentiment = 1 )                   as tweets_all_neu
     , count(DISTINCT d.twitter_id) filter ( where d.sentiment = 0 )                   as tweets_all_neg
     , count(DISTINCT d.twitter_id) filter ( where d.panel = 'A')                      as tweets_a
     , count(DISTINCT d.twitter_id) filter ( where d.panel = 'A' AND d.sentiment = 2)  as tweets_a_pos
     , count(DISTINCT d.twitter_id) filter ( where d.panel = 'A' AND d.sentiment = 1)  as tweets_a_neu
     , count(DISTINCT d.twitter_id) filter ( where d.panel = 'A' AND d.sentiment = 0 ) as tweets_a_neg
     , count(DISTINCT d.twitter_id) filter ( where d.panel = 'B')                      as tweets_b
     , count(DISTINCT d.twitter_id) filter ( where d.panel = 'B' AND d.sentiment = 2)  as tweets_b_pos
     , count(DISTINCT d.twitter_id) filter ( where d.panel = 'B' AND d.sentiment = 1)  as tweets_b_neu
     , count(DISTINCT d.twitter_id) filter ( where d.panel = 'B' AND d.sentiment = 0 ) as tweets_b_neg
     , count(DISTINCT d.twitter_id) filter ( where d.panel = 'C')                      as tweets_c
     , count(DISTINCT d.twitter_id) filter ( where d.panel = 'C' AND d.sentiment = 2)  as tweets_c_pos
     , count(DISTINCT d.twitter_id) filter ( where d.panel = 'C' AND d.sentiment = 1)  as tweets_c_neu
     , count(DISTINCT d.twitter_id) filter ( where d.panel = 'C' AND d.sentiment = 0 ) as tweets_c_neg
FROM dat d
GROUP BY d.technology
UNION
SELECT 100                                                                             as technology
     , count(DISTINCT d.twitter_id)                                                    as tweets_all
     , count(DISTINCT d.twitter_id) filter ( where d.sentiment = 2 )                   as tweets_all_pos
     , count(DISTINCT d.twitter_id) filter ( where d.sentiment = 1 )                   as tweets_all_neu
     , count(DISTINCT d.twitter_id) filter ( where d.sentiment = 0 )                   as tweets_all_neg
     , count(DISTINCT d.twitter_id) filter ( where d.panel = 'A')                      as tweets_a
     , count(DISTINCT d.twitter_id) filter ( where d.panel = 'A' AND d.sentiment = 2)  as tweets_a_pos
     , count(DISTINCT d.twitter_id) filter ( where d.panel = 'A' AND d.sentiment = 1)  as tweets_a_neu
     , count(DISTINCT d.twitter_id) filter ( where d.panel = 'A' AND d.sentiment = 0 ) as tweets_a_neg
     , count(DISTINCT d.twitter_id) filter ( where d.panel = 'B')                      as tweets_b
     , count(DISTINCT d.twitter_id) filter ( where d.panel = 'B' AND d.sentiment = 2)  as tweets_b_pos
     , count(DISTINCT d.twitter_id) filter ( where d.panel = 'B' AND d.sentiment = 1)  as tweets_b_neu
     , count(DISTINCT d.twitter_id) filter ( where d.panel = 'B' AND d.sentiment = 0 ) as tweets_b_neg
     , count(DISTINCT d.twitter_id) filter ( where d.panel = 'C')                      as tweets_c
     , count(DISTINCT d.twitter_id) filter ( where d.panel = 'C' AND d.sentiment = 2)  as tweets_c_pos
     , count(DISTINCT d.twitter_id) filter ( where d.panel = 'C' AND d.sentiment = 1)  as tweets_c_neu
     , count(DISTINCT d.twitter_id) filter ( where d.panel = 'C' AND d.sentiment = 0 ) as tweets_c_neg
FROM dat d
ORDER BY 1;


-- Tweet count per quarter, technology and panel
WITH buckets as (SELECT generate_series('2010-01-01 00:00'::timestamp,
                                        '2022-12-31 00:00'::timestamp,
                                        '3 months') as bucket),
     technologies as (SELECT DISTINCT value_int as technology
                      FROM bot_annotation
                      WHERE bot_annotation_metadata_id = 'fc73da56-9f51-4d2b-ad35-2a01dbe9b275'
                        AND key = 'tech'
                        AND value_int > 1),
     tweets AS (SELECT ti.item_id,
                       ti.twitter_id,
                       ti.created_at,
                       ti.twitter_author_id,
                       ba_tech.value_int                                                                   as technology,
                       ba_senti.value_int                                                                  as sentiment,
                       (ti."user" -> 'created_at')::text::timestamp                                        as created,
                       extract('day' from date_trunc('day', '2023-01-01'::timestamp -
                                                            (ti."user" -> 'created_at')::text::timestamp)) as days,
                       (ti."user" -> 'tweet_count')::int                                                   as n_tweets
                FROM twitter_item ti
                         LEFT JOIN bot_annotation ba_tech ON (
                            ti.item_id = ba_tech.item_id
                        AND ba_tech.bot_annotation_metadata_id = 'fc73da56-9f51-4d2b-ad35-2a01dbe9b275'
                        AND ba_tech.key = 'tech')
                         LEFT JOIN bot_annotation ba_senti ON (
                            ti.item_id = ba_senti.item_id
                        AND ba_senti.bot_annotation_metadata_id = 'e63da0c9-9bb5-4026-ab5e-7d5845cdc111'
                        AND ba_senti.key = 'senti'
                        AND ba_senti.repeat = 1)
                WHERE ti.project_id = 'c5d36b2e-cbb4-47a8-8370-e5f52bb78bf3'
                  AND ti.created_at >= '2010-01-01'::timestamp
                  AND ti.created_at <= '2023-01-01'::timestamp
                  AND ba_tech.value_int > 1),
     users_pre AS (SELECT twitter_author_id,
                          AVG(days)                  as days,
                          AVG(n_tweets)              as n_tweets,
                          COUNT(DISTINCT twitter_id) as n_cdr_tweets
                   FROM tweets
                   GROUP BY twitter_author_id),
     users AS (SELECT users_pre.*,
                      CASE
                          WHEN n_tweets / days <= 100
                              AND n_cdr_tweets <= 2 THEN 'A'
                          WHEN n_tweets / days <= 100
                              AND n_cdr_tweets > 2
                              AND n_cdr_tweets <= 50 THEN 'B'
                          WHEN n_tweets / days <= 100
                              AND n_cdr_tweets > 50 THEN 'C'
                          ELSE 'EX'
                          END as panel
               FROM users_pre),
     dat as (SELECT ti.created_at as created_at,
                    ti.twitter_id as twitter_id,
                    ti.sentiment  as sentiment,
                    ti.technology as technology,
                    u.panel       as panel
             FROM tweets ti
                      LEFT OUTER JOIN users u ON ti.twitter_author_id = u.twitter_author_id)
SELECT b.bucket                                                                        as bucket
     , t.technology                                                                    as technology
     , count(DISTINCT d.twitter_id)                                                    as tweets_all
     , count(DISTINCT d.twitter_id) filter ( where d.sentiment = 2 )                   as tweets_all_pos
     , count(DISTINCT d.twitter_id) filter ( where d.sentiment = 1 )                   as tweets_all_neu
     , count(DISTINCT d.twitter_id) filter ( where d.sentiment = 0 )                   as tweets_all_neg
     , count(DISTINCT d.twitter_id) filter ( where d.panel = 'A')                      as tweets_a
     , count(DISTINCT d.twitter_id) filter ( where d.panel = 'A' AND d.sentiment = 2)  as tweets_a_pos
     , count(DISTINCT d.twitter_id) filter ( where d.panel = 'A' AND d.sentiment = 1)  as tweets_a_neu
     , count(DISTINCT d.twitter_id) filter ( where d.panel = 'A' AND d.sentiment = 0 ) as tweets_a_neg
     , count(DISTINCT d.twitter_id) filter ( where d.panel = 'B')                      as tweets_b
     , count(DISTINCT d.twitter_id) filter ( where d.panel = 'B' AND d.sentiment = 2)  as tweets_b_pos
     , count(DISTINCT d.twitter_id) filter ( where d.panel = 'B' AND d.sentiment = 1)  as tweets_b_neu
     , count(DISTINCT d.twitter_id) filter ( where d.panel = 'B' AND d.sentiment = 0 ) as tweets_b_neg
     , count(DISTINCT d.twitter_id) filter ( where d.panel = 'C')                      as tweets_c
     , count(DISTINCT d.twitter_id) filter ( where d.panel = 'C' AND d.sentiment = 2)  as tweets_c_pos
     , count(DISTINCT d.twitter_id) filter ( where d.panel = 'C' AND d.sentiment = 1)  as tweets_c_neu
     , count(DISTINCT d.twitter_id) filter ( where d.panel = 'C' AND d.sentiment = 0 ) as tweets_c_neg
FROM (buckets b CROSS JOIN technologies t)
         LEFT JOIN dat d ON (
            d.created_at >= b.bucket
        AND d.created_at <= (b.bucket + '3 month'::interval)
        AND t.technology = d.technology)
GROUP BY t.technology, b.bucket
UNION
SELECT b.bucket                                                                        as bucket
     , 100                                                                             as technology
     , count(DISTINCT d.twitter_id)                                                    as tweets_all
     , count(DISTINCT d.twitter_id) filter ( where d.sentiment = 2 )                   as tweets_all_pos
     , count(DISTINCT d.twitter_id) filter ( where d.sentiment = 1 )                   as tweets_all_neu
     , count(DISTINCT d.twitter_id) filter ( where d.sentiment = 0 )                   as tweets_all_neg
     , count(DISTINCT d.twitter_id) filter ( where d.panel = 'A')                      as tweets_a
     , count(DISTINCT d.twitter_id) filter ( where d.panel = 'A' AND d.sentiment = 2)  as tweets_a_pos
     , count(DISTINCT d.twitter_id) filter ( where d.panel = 'A' AND d.sentiment = 1)  as tweets_a_neu
     , count(DISTINCT d.twitter_id) filter ( where d.panel = 'A' AND d.sentiment = 0 ) as tweets_a_neg
     , count(DISTINCT d.twitter_id) filter ( where d.panel = 'B')                      as tweets_b
     , count(DISTINCT d.twitter_id) filter ( where d.panel = 'B' AND d.sentiment = 2)  as tweets_b_pos
     , count(DISTINCT d.twitter_id) filter ( where d.panel = 'B' AND d.sentiment = 1)  as tweets_b_neu
     , count(DISTINCT d.twitter_id) filter ( where d.panel = 'B' AND d.sentiment = 0 ) as tweets_b_neg
     , count(DISTINCT d.twitter_id) filter ( where d.panel = 'C')                      as tweets_c
     , count(DISTINCT d.twitter_id) filter ( where d.panel = 'C' AND d.sentiment = 2)  as tweets_c_pos
     , count(DISTINCT d.twitter_id) filter ( where d.panel = 'C' AND d.sentiment = 1)  as tweets_c_neu
     , count(DISTINCT d.twitter_id) filter ( where d.panel = 'C' AND d.sentiment = 0 ) as tweets_c_neg
FROM buckets b
         LEFT JOIN dat d ON (
            d.created_at >= b.bucket
        AND d.created_at <= (b.bucket + '3 month'::interval))
GROUP BY b.bucket
ORDER BY 2, 1;



WITH tweets AS (SELECT ti.item_id,
                       ti.twitter_id,
                       ti.created_at,
                       ti.twitter_author_id,
                       ti.like_count,
                       ti.reply_count,
                       ti.retweet_count,
                       ba_tech.value_int                                                                   as technology,
                       (ti."user" -> 'created_at')::text::timestamp                                        as created,
                       extract('day' from date_trunc('day', '2023-01-01'::timestamp -
                                                            (ti."user" -> 'created_at')::text::timestamp)) as days,
                       (ti."user" -> 'tweet_count')::int                                                   as n_tweets
                FROM twitter_item ti
                         LEFT JOIN bot_annotation ba_tech ON (
                            ti.item_id = ba_tech.item_id
                        AND ba_tech.bot_annotation_metadata_id = 'fc73da56-9f51-4d2b-ad35-2a01dbe9b275'
                        AND ba_tech.key = 'tech')
                WHERE ti.project_id = 'c5d36b2e-cbb4-47a8-8370-e5f52bb78bf3'
                  AND ti.created_at >= '2010-01-01'::timestamp
                  AND ti.created_at <= '2023-01-01'::timestamp),
     users_pre AS (SELECT twitter_author_id,
                          AVG(days)                                                as days,
                          AVG(n_tweets)                                            as n_tweets,
                          COUNT(DISTINCT twitter_id)                               as n_cdr_tweets,
                          COUNT(DISTINCT twitter_id) FILTER (WHERE technology > 1) as n_cdr_tweets_noccs
                   FROM tweets
                   GROUP BY twitter_author_id),
     users AS (SELECT users_pre.*,
                      CASE
                          WHEN n_tweets / days <= 100
                              AND n_cdr_tweets <= 2 THEN 'A'
                          WHEN n_tweets / days <= 100
                              AND n_cdr_tweets > 2
                              AND n_cdr_tweets <= 50 THEN 'B'
                          WHEN n_tweets / days <= 100
                              AND n_cdr_tweets > 50 THEN 'C'
                          ELSE 'EX'
                          END as panel
               FROM users_pre)
SELECT ti.technology              as technology,
       panel,
       COUNT(DISTINCT twitter_id) as n_tweets,
       AVG(like_count)            as avg_likes,
       AVG(retweet_count)         as avg_rt,
       AVG(reply_count)           as avg_reply,
       SUM(like_count)            as sum_likes,
       SUM(retweet_count)         as sum_rt,
       SUM(reply_count)           as sum_reply
FROM tweets ti
         LEFT OUTER JOIN users u ON ti.twitter_author_id = u.twitter_author_id
GROUP BY ti.technology, panel
UNION
SELECT ti.technology              as technology,
       'All'                      as panel,
       COUNT(DISTINCT twitter_id) as n_tweets,
       AVG(like_count)            as avg_likes,
       AVG(retweet_count)         as avg_rt,
       AVG(reply_count)           as avg_reply,
       SUM(like_count)            as sum_likes,
       SUM(retweet_count)         as sum_rt,
       SUM(reply_count)           as sum_reply
FROM tweets ti
         LEFT OUTER JOIN users u ON ti.twitter_author_id = u.twitter_author_id
GROUP BY ti.technology
UNION
SELECT 100                        as technology,
       panel,
       COUNT(DISTINCT twitter_id) as n_tweets,
       AVG(like_count)            as avg_likes,
       AVG(retweet_count)         as avg_rt,
       AVG(reply_count)           as avg_reply,
       SUM(like_count)            as sum_likes,
       SUM(retweet_count)         as sum_rt,
       SUM(reply_count)           as sum_reply
FROM tweets ti
         LEFT OUTER JOIN users u ON ti.twitter_author_id = u.twitter_author_id
WHERE technology > 1
GROUP BY panel
UNION
SELECT 100                        as technology,
       'All'                      as panel,
       COUNT(DISTINCT twitter_id) as n_tweets,
       AVG(like_count)            as avg_likes,
       AVG(retweet_count)         as avg_rt,
       AVG(reply_count)           as avg_reply,
       SUM(like_count)            as sum_likes,
       SUM(retweet_count)         as sum_rt,
       SUM(reply_count)           as sum_reply
FROM tweets ti
         LEFT OUTER JOIN users u ON ti.twitter_author_id = u.twitter_author_id
WHERE technology > 1
UNION
SELECT 200                        as technology,
       panel,
       COUNT(DISTINCT twitter_id) as n_tweets,
       AVG(like_count)            as avg_likes,
       AVG(retweet_count)         as avg_rt,
       AVG(reply_count)           as avg_reply,
       SUM(like_count)            as sum_likes,
       SUM(retweet_count)         as sum_rt,
       SUM(reply_count)           as sum_reply
FROM tweets ti
         LEFT OUTER JOIN users u ON ti.twitter_author_id = u.twitter_author_id
GROUP BY panel
UNION
SELECT 200                        as technology,
       'All'                      as panel,
       COUNT(DISTINCT twitter_id) as n_tweets,
       AVG(like_count)            as avg_likes,
       AVG(retweet_count)         as avg_rt,
       AVG(reply_count)           as avg_reply,
       SUM(like_count)            as sum_likes,
       SUM(retweet_count)         as sum_rt,
       SUM(reply_count)           as sum_reply
FROM tweets ti
         LEFT OUTER JOIN users u ON ti.twitter_author_id = u.twitter_author_id
ORDER BY 1, 2;


WITH tweets AS (SELECT ti.item_id,
                                   ti.twitter_id,
                                   ti.created_at,
                                   ti.twitter_author_id,
                                   ba_tech.value_int                                                                   as technology,
                                   (ti."user" -> 'created_at')::text::timestamp                                        as created,
                                   extract('day' from date_trunc('day', '2023-01-01'::timestamp -
                                                                        (ti."user" -> 'created_at')::text::timestamp)) as days,
                                   (ti."user" -> 'tweet_count')::int                                                   as n_tweets
                            FROM twitter_item ti
                                     LEFT JOIN bot_annotation ba_tech ON (
                                        ti.item_id = ba_tech.item_id
                                    AND ba_tech.bot_annotation_metadata_id = 'fc73da56-9f51-4d2b-ad35-2a01dbe9b275'
                                    AND ba_tech.key = 'tech')
                            WHERE ti.project_id = 'c5d36b2e-cbb4-47a8-8370-e5f52bb78bf3'
                              AND ti.created_at >= '2006-01-01'::timestamp
                              AND ti.created_at <= '2023-01-01'::timestamp),
                 users_pre AS (SELECT twitter_author_id,
                                      AVG(days)                                                as days,
                                      AVG(n_tweets)                                            as n_tweets,
                                      COUNT(DISTINCT twitter_id)                               as n_cdr_tweets,
                                      COUNT(DISTINCT twitter_id) FILTER (WHERE technology > 1) as n_cdr_tweets_noccs
                               FROM tweets
                               GROUP BY twitter_author_id),
                 users AS (SELECT users_pre.*,
                                  CASE
                                      WHEN n_tweets / days <= 100
                                          AND n_cdr_tweets_noccs <= 2 THEN 'A'
                                      WHEN n_tweets / days <= 100
                                          AND n_cdr_tweets_noccs > 2
                                          AND n_cdr_tweets_noccs <= 50 THEN 'B'
                                      WHEN n_tweets / days <= 100
                                          AND n_cdr_tweets_noccs > 50 THEN 'C'
                                      ELSE 'EX'
                                      END as panel
                           FROM users_pre)
select panel, count(distinct twitter_author_id)
FROM users
GROUP BY panel;