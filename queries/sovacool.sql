WITH tweets AS (SELECT ti.item_id,
                       ti.twitter_id,
                       ti.created_at,
                       ti.twitter_author_id,
                       ti.like_count,
                       ti.reply_count,
                       ti.retweet_count,
                       (ti."user" -> 'followers_count')::int                                               as n_followers,
                       (ti."user" -> 'username')::text                                                     as username,
                       (ti."user" -> 'name')::text                                                         as name,
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
                          MAX(username)                                            as username,
                          MAX(name)                                                as name,
                          MAX(n_followers)                                         as n_followers,
                          MIN(created_at)                                          as created_at,
                          MAX(days)                                                as days_active,
                          MAX(n_tweets)                                            as n_tweets,
                          COUNT(DISTINCT twitter_id)                               as n_cdr_tweets,
                          COUNT(DISTINCT twitter_id) FILTER (WHERE technology > 1) as n_cdr_tweets_noccs,
                          COUNT(DISTINCT twitter_id) / AVG(days)                   as cdr_tweets_per_day,
                          AVG(n_tweets) / AVG(days)                                as tweets_per_day
                   FROM tweets
                   GROUP BY twitter_author_id)
SELECT *
FROM users_pre
WHERE n_followers > 5000
   OR cdr_tweets_per_day > 0.14
ORDER BY n_cdr_tweets desc;


WITH tweets AS (SELECT ti.item_id,
                       it.text,
                       ti.twitter_id,
                       ti.created_at,
                       ti.twitter_author_id,
                       ti.like_count,
                       ti.reply_count,
                       ti.retweet_count,
                       (ti."user" -> 'followers_count')::int                                               as n_followers,
                       (ti."user" -> 'username')::text                                                     as username,
                       (ti."user" -> 'name')::text                                                         as name,
                       ba_tech.value_int                                                                   as technology,
                       (ti."user" -> 'created_at')::text::timestamp                                        as created,
                       extract('day' from date_trunc('day', '2023-01-01'::timestamp -
                                                            (ti."user" -> 'created_at')::text::timestamp)) as days,
                       (ti."user" -> 'tweet_count')::int                                                   as n_tweets
                FROM twitter_item ti
                         JOIN item it ON it.item_id = ti.item_id
                         LEFT JOIN bot_annotation ba_tech ON (
                    ti.item_id = ba_tech.item_id
                        AND ba_tech.bot_annotation_metadata_id = 'fc73da56-9f51-4d2b-ad35-2a01dbe9b275'
                        AND ba_tech.key = 'tech')
                WHERE ti.project_id = 'c5d36b2e-cbb4-47a8-8370-e5f52bb78bf3'
                  AND ti.created_at >= '2010-01-01'::timestamp
                  AND ti.created_at <= '2023-01-01'::timestamp),
     users_pre AS (SELECT twitter_author_id,
                          MAX(username)                                            as username,
                          MAX(name)                                                as name,
                          MAX(n_followers)                                         as n_followers,
                          MIN(created_at)                                          as created_at,
                          MAX(days)                                                as days_active,
                          MAX(n_tweets)                                            as n_tweets,
                          COUNT(DISTINCT twitter_id)                               as n_cdr_tweets,
                          COUNT(DISTINCT twitter_id) FILTER (WHERE technology > 1) as n_cdr_tweets_noccs,
                          COUNT(DISTINCT twitter_id) / AVG(days)                   as cdr_tweets_per_day,
                          AVG(n_tweets) / AVG(days)                                as tweets_per_day
                   FROM tweets
                   GROUP BY twitter_author_id),
     users_filtered AS (SELECT *
                        FROM users_pre
                        WHERE n_followers > 5000
                          OR cdr_tweets_per_day > 0.14)
SELECT distinct item_id, *
FROM tweets ti
         JOIN users_filtered u ON ti.twitter_author_id = u.twitter_author_id;

SELECT count(1)
FROM twitter_item ti
WHERE ti.project_id = 'c5d36b2e-cbb4-47a8-8370-e5f52bb78bf3';



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