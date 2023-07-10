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
                  AND ti.created_at >= '2010-01-01'::timestamp
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
     dat AS (SELECT ti.created_at        as created_at,
                    ti.twitter_id        as twitter_id,
                    ti.twitter_author_id as twitter_author_id,
                    ti.sentiment         as sentiment,
                    ti.technology        as technology,
                    u.panel              as panel
             FROM tweets ti
                      LEFT OUTER JOIN users u ON ti.twitter_author_id = u.twitter_author_id
             WHERE u.panel = 'C'),
     lookup AS (SELECT d.twitter_author_id                         as author
                     , count(1) FILTER ( WHERE d.technology = 2 )  as t2
                     , count(1) FILTER ( WHERE d.technology = 3 )  as t3
                     , count(1) FILTER ( WHERE d.technology = 4 )  as t4
                     , count(1) FILTER ( WHERE d.technology = 5 )  as t5
                     , count(1) FILTER ( WHERE d.technology = 6 )  as t6
                     , count(1) FILTER ( WHERE d.technology = 7 )  as t7
                     , count(1) FILTER ( WHERE d.technology = 8 )  as t8
                     , count(1) FILTER ( WHERE d.technology = 9 )  as t9
                     , count(1) FILTER ( WHERE d.technology = 10 ) as t10
                     , count(1) FILTER ( WHERE d.technology = 11 ) as t11
                     , count(1) FILTER ( WHERE d.technology = 12 ) as t12
                FROM dat d
                GROUP BY d.twitter_author_id)

SELECT *
FROM lookup l
LIMIT 100;


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
                  AND ti.created_at >= '2010-01-01'::timestamp
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
     dat AS (SELECT ti.created_at        as created_at,
                    ti.twitter_id        as twitter_id,
                    ti.twitter_author_id as twitter_author_id,
                    ti.sentiment         as sentiment,
                    ti.technology        as technology,
                    u.panel              as panel
             FROM tweets ti
                      LEFT OUTER JOIN users u ON ti.twitter_author_id = u.twitter_author_id
             WHERE u.panel = 'C'),
     techs AS (SELECT DISTINCT technology FROM dat)
SELECT t1.technology                         as te1
     , t2.technology                         as te2
     , (SELECT count(DISTINCT d1.twitter_author_id)
        FROM dat d1
                 JOIN dat d2 ON d1.twitter_author_id = d2.twitter_author_id
        WHERE t1.technology = d1.technology
          AND t2.technology = d2.technology) as cnt
FROM techs t1
         JOIN techs t2 ON t1.technology <= t2.technology
ORDER BY te1, te2;


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
                  AND ti.created_at >= '2010-01-01'::timestamp
                  AND ti.created_at <= '2023-01-01'::timestamp
                  AND ba_tech.value_int > 1),
     users_pre AS (SELECT twitter_author_id,
                          AVG(days)                  as days,
                          AVG(n_tweets)              as n_tweets,
                          COUNT(DISTINCT twitter_id) as n_cdr_tweets
                   FROM tweets
                   GROUP BY twitter_author_id),
     dat AS (SELECT ti.created_at        as created_at,
                    ti.twitter_id        as twitter_id,
                    ti.twitter_author_id as twitter_author_id,
                    ti.technology        as technology
             FROM users_pre up
                      LEFT JOIN tweets ti ON ti.twitter_author_id = up.twitter_author_id
             WHERE up.n_tweets / up.days <= 100
               AND up.n_cdr_tweets > 50),
     techs AS (SELECT DISTINCT technology FROM dat)
SELECT t1.technology                         as te1
     , t2.technology                         as te2
     , (SELECT count(DISTINCT d1.twitter_author_id)
        FROM dat d1
                 JOIN dat d2 ON d1.twitter_author_id = d2.twitter_author_id
        WHERE t1.technology = d1.technology
          AND t2.technology = d2.technology) as cnt
FROM techs t1
         JOIN techs t2 ON t1.technology <= t2.technology
ORDER BY te1, te2;

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
                  AND ti.created_at >= '2010-01-01'::timestamp
                  AND ti.created_at <= '2023-01-01'::timestamp
                  AND ba_tech.value_int > 1),
     users_pre AS (SELECT twitter_author_id,
                          AVG(days)                  as days,
                          AVG(n_tweets)              as n_tweets,
                          COUNT(DISTINCT twitter_id) as n_cdr_tweets
                   FROM tweets
                   GROUP BY twitter_author_id),
     dat AS (SELECT ti.created_at        as created_at,
                    ti.twitter_id        as twitter_id,
                    ti.twitter_author_id as twitter_author_id,
                    ti.technology        as technology
             FROM users_pre up
                      LEFT JOIN tweets ti ON ti.twitter_author_id = up.twitter_author_id
             WHERE up.n_tweets / up.days <= 100
               AND up.n_cdr_tweets > 2
               AND up.n_cdr_tweets <= 50)
        ,
     techs AS (SELECT DISTINCT technology FROM dat)
        ,
     couples as
         (select d1.technology                        as tec1
               , d2.technology                        as tec2
               , count(distinct d1.twitter_author_id) as cnt
          from dat d1
                   join dat d2
                        on d1.technology <= d2.technology
                            and d1.twitter_author_id = d2.twitter_author_id
          GROUP BY d1.technology, d2.technology)
SELECT t1.technology      as te1,
       t2.technology      as te2,
       coalesce(c.cnt, 0) as cnt
FROM techs t1
         join techs t2 ON t1.technology < t2.technology
         LEFT JOIN couples c ON t1.technology = c.tec1 AND t2.technology = c.tec2;


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
                  AND ti.created_at >= '2010-01-01'::timestamp
                  AND ti.created_at <= '2023-01-01'::timestamp
                  AND ba_tech.value_int > 1),
     techs AS (SELECT DISTINCT technology FROM tweets),
     couples as
         (select d1.technology                 as tec1
               , d2.technology                 as tec2
               , count(distinct d1.twitter_id) as cnt
          from tweets d1
                   join tweets d2
                        on d1.technology <= d2.technology
                            and d1.twitter_author_id = d2.twitter_author_id
          GROUP BY d1.technology, d2.technology)
SELECT t1.technology      as te1,
       t2.technology      as te2,
       coalesce(c.cnt, 0) as cnt
FROM techs t1
         join techs t2 ON t1.technology < t2.technology
         LEFT JOIN couples c ON t1.technology = c.tec1 AND t2.technology = c.tec2;



WITH tweets AS (SELECT ti.item_id,
                       ti.twitter_id,
                       ba_tech.value_int as technology
                FROM twitter_item ti
                         LEFT JOIN bot_annotation ba_tech ON (
                            ti.item_id = ba_tech.item_id
                        AND ba_tech.bot_annotation_metadata_id = 'fc73da56-9f51-4d2b-ad35-2a01dbe9b275'
                        AND ba_tech.key = 'tech')
                WHERE ti.project_id = 'c5d36b2e-cbb4-47a8-8370-e5f52bb78bf3'
                  AND ti.created_at >= '2010-01-01'::timestamp
                  AND ti.created_at <= '2023-01-01'::timestamp
                  AND ba_tech.value_int > 1),
    ntech AS (SELECT count(1) as cnt
              FROM tweets
              GROUP BY twitter_id)
SELECT cnt as num, count(1) as n_tweets
FROM ntech
GROUP BY cnt
ORDER BY num;