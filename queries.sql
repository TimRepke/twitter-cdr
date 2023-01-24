-- Project ID: e63da0c9-9bb5-4026-ab5e-7d5845cdc111

-- Query annotation scheme: d83a736a-791f-4611-9c44-002cb4056202
-- Query bot annotations: fc73da56-9f51-4d2b-ad35-2a01dbe9b275

-- Sentiment and emotion annotation scheme: 8d5d899f-b615-4962-b468-d95893df0921
-- Sentiment bot annotations: e63da0c9-9bb5-4026-ab5e-7d5845cdc111

-- Number of tweets and users in the project (incl. avg num tweets per user)
SELECT count(DISTINCT ti.twitter_id)                                                      as num_tweets,
       count(DISTINCT ti.twitter_author_id)                                               as num_users,
       count(DISTINCT ti.twitter_id)::float / count(DISTINCT ti.twitter_author_id)::float as tweets_per_user
FROM twitter_item ti
WHERE ti.project_id = 'c5d36b2e-cbb4-47a8-8370-e5f52bb78bf3';

-- Number of tweets primarily classified as negative(0), neutral(1), positive(2)
SELECT value_int, count(1) as num_tweets
FROM bot_annotation
WHERE key = 'senti'
  AND repeat = 1
  AND bot_annotation_metadata_id = 'e63da0c9-9bb5-4026-ab5e-7d5845cdc111'
GROUP BY value_int;

-- Number of tweets per language
SELECT language, count(1) as num_tweets
FROM twitter_item
GROUP BY language
ORDER BY num_tweets DESC;

-- Temporal histogram (1-day resolution) of tweet counts
WITH buckets as (SELECT to_char(generate_series('2006-01-01 00:00'::timestamp,
                                                '2022-12-31 23:59'::timestamp,
                                                '1 day'), 'YYYY-MM-DD') as bucket),
     ti as (SELECT to_char(twitter_item.created_at, 'YYYY-MM-DD') as day
            FROM twitter_item
            WHERE twitter_item.project_id = 'c5d36b2e-cbb4-47a8-8370-e5f52bb78bf3')
SELECT b.bucket as day, count(ti.day) as num_tweets
FROM buckets b
         LEFT JOIN ti ON ti.day = b.bucket
GROUP BY b.bucket
ORDER BY day;

-- Temporal histogram (1-day resolution) of tweet counts
-- (incl number of positive/neutral/negative primary sentiment label counts)
WITH buckets as (SELECT to_char(generate_series('2006-01-01 00:00'::timestamp,
                                                '2022-12-31 23:59'::timestamp,
                                                '1 day'), 'YYYY-MM-DD') as bucket),
     labels as (SELECT to_char(twitter_item.created_at, 'YYYY-MM-DD') as day, ba.value_int as sentiment
                FROM twitter_item
                         LEFT JOIN bot_annotation ba on twitter_item.item_id = ba.item_id
                WHERE twitter_item.project_id = 'c5d36b2e-cbb4-47a8-8370-e5f52bb78bf3'
                  AND ba.bot_annotation_metadata_id = 'e63da0c9-9bb5-4026-ab5e-7d5845cdc111'
                  AND ba.repeat = 1
                  AND ba.key = 'senti')
SELECT b.bucket                                     as day,
       count(labels.sentiment)                      as num_tweets,
       count(1) FILTER (WHERE labels.sentiment = 0) as num_negative,
       count(1) FILTER (WHERE labels.sentiment = 1) as num_neutral,
       count(1) FILTER (WHERE labels.sentiment = 2) as num_positive
FROM buckets b
         LEFT JOIN labels ON labels.day = b.bucket
GROUP BY b.bucket
ORDER BY day;

-- Temporal histogram (1-day resolution) of tweet counts (incl number of tweets per technology)
WITH buckets as (SELECT to_char(generate_series('2006-01-01 00:00'::timestamp,
                                                '2022-12-31 23:59'::timestamp,
                                                '1 day'), 'YYYY-MM-DD') as bucket),
     labels as (SELECT to_char(twitter_item.created_at, 'YYYY-MM-DD') as day,
                       twitter_item.twitter_id                        as twitter_id,
                       ba.value_int                                   as technology
                FROM twitter_item
                         LEFT JOIN bot_annotation ba on twitter_item.item_id = ba.item_id
                WHERE twitter_item.project_id = 'c5d36b2e-cbb4-47a8-8370-e5f52bb78bf3'
                  AND ba.bot_annotation_metadata_id = 'fc73da56-9f51-4d2b-ad35-2a01dbe9b275'
                  AND ba.key = 'tech')
SELECT b.bucket                                                                as day,
       count(labels.technology)                                                as num_tweets,
       count(DISTINCT labels.twitter_id) FILTER (WHERE labels.technology = 0)  as "Methane Removal",
       count(DISTINCT labels.twitter_id) FILTER (WHERE labels.technology = 1)  as "CCS",
       count(DISTINCT labels.twitter_id) FILTER (WHERE labels.technology = 2)  as "Ocean Fertilization",
       count(DISTINCT labels.twitter_id) FILTER (WHERE labels.technology = 3)  as "Ocean Alkalinization",
       count(DISTINCT labels.twitter_id) FILTER (WHERE labels.technology = 4)  as "Enhanced Weathering",
       count(DISTINCT labels.twitter_id) FILTER (WHERE labels.technology = 5)  as "Biochar",
       count(DISTINCT labels.twitter_id) FILTER (WHERE labels.technology = 6)  as "Afforestation/Reforestation",
       count(DISTINCT labels.twitter_id) FILTER (WHERE labels.technology = 7)  as "Ecosystem Restoration",
       count(DISTINCT labels.twitter_id) FILTER (WHERE labels.technology = 8)  as "Soil Carbon Sequestration",
       count(DISTINCT labels.twitter_id) FILTER (WHERE labels.technology = 9)  as "BECCS",
       count(DISTINCT labels.twitter_id) FILTER (WHERE labels.technology = 10) as "Blue Carbon",
       count(DISTINCT labels.twitter_id) FILTER (WHERE labels.technology = 11) as "Direct Air Capture",
       count(DISTINCT labels.twitter_id) FILTER (WHERE labels.technology = 12) as "GGR (general)"
FROM buckets b
         LEFT JOIN labels ON labels.day = b.bucket
GROUP BY b.bucket
ORDER BY day;


-- Temporal histogram (1-day resolution) of tweet counts for a technology
-- (incl number of positive/neutral/negative primary sentiment label counts)
WITH buckets as (SELECT generate_series('2006-01-01 00:00'::timestamp,
                                        '2022-12-31 23:59'::timestamp,
                                        '1 week') as bucket),
     labels as (SELECT DISTINCT ON (twitter_item.twitter_id, ba_tech.value_int) twitter_item.created_at,
                                                                                to_char(twitter_item.created_at, 'YYYY-MM-DD') as day,
                                                                                ba_sent.value_int                              as sentiment,
                                                                                ba_tech.value_int                              as technology
                FROM twitter_item
                         LEFT OUTER JOIN bot_annotation ba_tech on (
                            twitter_item.item_id = ba_tech.item_id
                        AND ba_tech.bot_annotation_metadata_id = 'fc73da56-9f51-4d2b-ad35-2a01dbe9b275'
                        AND ba_tech.key = 'tech')
                         LEFT JOIN bot_annotation ba_sent on (
                            ba_tech.item_id = ba_sent.item_id
                        AND ba_sent.bot_annotation_metadata_id = 'e63da0c9-9bb5-4026-ab5e-7d5845cdc111'
                        AND ba_sent.repeat = 1
                        AND ba_sent.key = 'senti')
                WHERE twitter_item.project_id = 'c5d36b2e-cbb4-47a8-8370-e5f52bb78bf3')
SELECT b.bucket                                     as day,
       count(labels.sentiment)                      as num_tweets,
       count(1) FILTER (WHERE labels.sentiment = 0) as num_negative,
       count(1) FILTER (WHERE labels.sentiment = 1) as num_neutral,
       count(1) FILTER (WHERE labels.sentiment = 2) as num_positive
FROM buckets b
         LEFT JOIN labels ON (
            labels.created_at >= b.bucket
        AND labels.created_at < b.bucket + interval '1 week'
        AND labels.technology = 2
    )
GROUP BY b.bucket
ORDER BY day;


-- Users with most CDR tweets
SELECT ti.twitter_author_id,
       u.username,
       -- Number of tweets matching any CDR query
       count(1)                                                as num_cdr_tweets,
       -- Tweets that are actually written and not just retweeted or quoted
       count(1) FILTER ( WHERE ti.referenced_tweets = 'null' ) as num_orig_cdr_tweets,
       -- Total number of tweets by the user (as per Twitters profile information)
       u.tweet_count,
       u.listed_count,
       u.followers_count,
       u.following_count,
       u.name,
       u.location,
       min(ti.created_at)                                      as earliest_cdr_tweet,
       max(ti.created_at)                                      as latest_cdr_tweet,
       u.created_at,
       u.verified,
       u.description
FROM twitter_item ti,
     jsonb_to_record(ti."user") as u (
                                      "name" text,
                                      "username" text,
                                      "location" text,
                                      "tweet_count" int,
                                      "listed_count" int,
                                      "followers_count" int,
                                      "following_count" int,
                                      "created_at" timestamp,
                                      "verified" bool,
                                      "description" text
         )

WHERE ti.project_id = 'c5d36b2e-cbb4-47a8-8370-e5f52bb78bf3'
GROUP BY u.name, u.username, u.location, u.tweet_count, u.listed_count, u.followers_count, u.following_count,
         u.created_at, u.verified, u.description, ti.twitter_author_id
ORDER BY num_cdr_tweets DESC
LIMIT 200;


-- Users with most CDR tweets (incl per technology count)
WITH user_tweets as (SELECT ti.item_id,
                            ti.twitter_id,
                            ti.twitter_author_id,
                            u.tweet_count,
                            u.listed_count,
                            u.followers_count,
                            u.following_count,
                            u.name,
                            u.username,
                            u.location,
                            u.created_at,
                            u.verified,
                            u.description,
                            ti.referenced_tweets = 'null' as is_orig
                     FROM twitter_item ti,
                          jsonb_to_record(ti."user") as u (
                                                           "name" text,
                                                           "username" text,
                                                           "location" text,
                                                           "tweet_count" int,
                                                           "listed_count" int,
                                                           "followers_count" int,
                                                           "following_count" int,
                                                           "created_at" timestamp,
                                                           "verified" bool,
                                                           "description" text
                              )
                     WHERE ti.project_id = 'c5d36b2e-cbb4-47a8-8370-e5f52bb78bf3')
SELECT ut.twitter_author_id,
       ut.username,
       -- Number of tweets matching any CDR query
       count(DISTINCT ut.twitter_id)                                      as num_cdr_tweets,
       -- Tweets that are actually written and not just retweeted or quoted
       count(DISTINCT ut.twitter_id) FILTER ( WHERE ut.is_orig )          as num_orig_cdr_tweets,
       -- Total number of tweets by the user (as per Twitters profile information)
       ut.tweet_count                                                     as num_tweets,
       (count(DISTINCT ut.twitter_id) FILTER ( WHERE ut.is_orig ))::float /
       count(DISTINCT ut.twitter_id)::float * 100                         as perc_orig,
       count(DISTINCT ut.twitter_id)::float / ut.tweet_count::float * 100 as perc_cdr,
       count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 0)      as "Methane Removal",
       count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 1)      as "CCS",
       count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 2)      as "Ocean Fertilization",
       count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 3)      as "Ocean Alkalinization",
       count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 4)      as "Enhanced Weathering",
       count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 5)      as "Biochar",
       count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 6)      as "Afforestation/Reforestation",
       count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 7)      as "Ecosystem Restoration",
       count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 8)      as "Soil Carbon Sequestration",
       count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 9)      as "BECCS",
       count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 10)     as "Blue Carbon",
       count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 11)     as "Direct Air Capture",
       count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 12)     as "GGR (general)",
       ut.tweet_count,
       ut.listed_count,
       ut.followers_count,
       ut.following_count,
       ut.name,
       ut.location,
       min(ut.created_at)                                                 as earliest_cdr_tweet,
       max(ut.created_at)                                                 as latest_cdr_tweet,
       ut.created_at,
       ut.verified,
       ut.description
FROM user_tweets ut
         LEFT JOIN bot_annotation ba on (ut.item_id = ba.item_id
    AND ba.bot_annotation_metadata_id = 'fc73da56-9f51-4d2b-ad35-2a01dbe9b275'
    AND ba.key = 'tech')
GROUP BY ut.name, ut.username, ut.location, ut.tweet_count, ut.listed_count, ut.followers_count, ut.following_count,
         ut.created_at, ut.verified, ut.description, ut.twitter_author_id
ORDER BY num_cdr_tweets DESC
LIMIT 200;

-- Number of tweets per query for a specific user
SELECT ti.twitter_author_id          as user_id,
       ti."user" -> 'username'       as username,
       ba.key                        as tech_category,
       ba.value_int                  as subquery,
       count(DISTINCT ti.twitter_id) as num_tweets
FROM twitter_item ti
         LEFT JOIN bot_annotation ba on (ti.item_id = ba.item_id
    AND ba.bot_annotation_metadata_id = 'fc73da56-9f51-4d2b-ad35-2a01dbe9b275'
    AND ba.key <> 'tech')
WHERE ti.twitter_author_id = '2863574275'
GROUP BY user_id, username, tech_category, subquery
ORDER BY num_tweets DESC;


-- Sentiment counts for the users with the most CDR tweets
SELECT ti.twitter_author_id                                            as user_id,
       ti."user" -> 'username'                                         as username,
       count(DISTINCT ti.twitter_id)                                   as num_cdr_tweets,
       count(DISTINCT ti.twitter_id) FILTER ( WHERE ba.value_int = 0 ) as num_neg,
       count(DISTINCT ti.twitter_id) FILTER ( WHERE ba.value_int = 1 ) as num_neu,
       count(DISTINCT ti.twitter_id) FILTER ( WHERE ba.value_int = 2 ) as num_pos
FROM twitter_item ti
         LEFT JOIN bot_annotation ba on (ti.item_id = ba.item_id
    AND ba.bot_annotation_metadata_id = 'e63da0c9-9bb5-4026-ab5e-7d5845cdc111'
    AND ba.key = 'senti'
    AND ba.repeat = 1)
-- WHERE ti.twitter_author_id = '2863574275'
GROUP BY user_id, username
ORDER BY num_cdr_tweets DESC
LIMIT 200;

-- Tweets with a certain primary sentiment of a specific user
WITH r as (SELECT DISTINCT ON (ti.twitter_id) ti.twitter_author_id    as user_id,
                                              ti.twitter_id           as tweet_id,
                                              ti."user" -> 'username' as username,
                                              ti.created_at           as posted,
                                              i.text                  as tweet
           FROM twitter_item ti
                    LEFT JOIN item i on i.item_id = ti.item_id
                    LEFT JOIN bot_annotation ba on (ti.item_id = ba.item_id
               AND ba.bot_annotation_metadata_id = 'e63da0c9-9bb5-4026-ab5e-7d5845cdc111'
               AND ba.key = 'senti'
               AND ba.repeat = 1)
           WHERE ti.twitter_author_id IN ('2863574275', '917421961', '3164575344', '610232748')
             AND ba.value_int = 0)
SELECT *
FROM r
ORDER BY posted
LIMIT 200;


-- Data export
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
SELECT ti.*, i.text, s.sentiment, s.sentiment_score, t.technologies
FROM twitter_item ti
         LEFT JOIN item i on i.item_id = ti.item_id
         LEFT JOIN sentiments s ON s.item_id = ti.item_id
         LEFT JOIN technologies t ON t.item_id = ti.item_id
WHERE ti.project_id = 'c5d36b2e-cbb4-47a8-8370-e5f52bb78bf3'
LIMIT 10;


-- URLs in tweets
SELECT ti.twitter_id, jsonb_path_query(ti.urls, '$[*].url_expanded')
FROM twitter_item ti
WHERE ti.project_id = 'c5d36b2e-cbb4-47a8-8370-e5f52bb78bf3'
LIMIT 100;


WITH urls as (SELECT ti."user" ->> 'username'                       as username,
                     jsonb_path_query(ti.urls, '$[*].url_expanded') as url
              FROM twitter_item ti
              WHERE ti.project_id = 'c5d36b2e-cbb4-47a8-8370-e5f52bb78bf3')
SELECT username, url, count(1) AS num_tweeted
FROM urls
GROUP BY username, url
LIMIT 10;


WITH urls as (SELECT ti."user" -> 'username'                        as username,
                     jsonb_path_query(ti.urls, '$[*].url_expanded') as url
              FROM twitter_item ti
              WHERE ti.project_id = 'c5d36b2e-cbb4-47a8-8370-e5f52bb78bf3'),
     uu_cnt as (SELECT username, url, count(1) AS num_tweeted
                FROM urls
                GROUP BY username, url)
SELECT url, json_object_agg(username, num_tweeted) AS user_stats
FROM uu_cnt
GROUP BY url
LIMIT 10;

SELECT uci.url,
       json_object_agg(uci.username, uci.num_tweeted) AS user_stats
FROM (SELECT u.username, u.url, count(1) AS num_tweeted
      FROM (SELECT ti."user" ->> 'username'                       as username,
                   jsonb_path_query(ti.urls, '$[*].url_expanded') as url
            FROM twitter_item ti
            WHERE ti.project_id = 'c5d36b2e-cbb4-47a8-8370-e5f52bb78bf3') u
      GROUP BY u.username, u.url) uci
GROUP BY uci.url
LIMIT 10;

-- Repeated URLs in tweets
WITH urls as (SELECT ti.twitter_id,
                     ti.twitter_author_id,
                     ti.created_at,
                     ti."user" ->> 'username'                       as username,
                     jsonb_path_query(ti.urls, '$[*].url_expanded') as url
              FROM twitter_item ti
              WHERE ti.project_id = 'c5d36b2e-cbb4-47a8-8370-e5f52bb78bf3')
SELECT urls.url,
       count(DISTINCT twitter_id)                                             as num_tweets,
       count(DISTINCT twitter_author_id)                                      as num_users,
       min(created_at)                                                        as first_posted,
       max(created_at)                                                        as last_posted,
       EXTRACT(epoch FROM (max(created_at) - min(created_at))) / 60 / 60 / 24 as lifetime_days,
       justify_interval(max(created_at) - min(created_at))                    as lifetime
--        array_agg(DISTINCT urls.username)                                      as users,
--        array_agg(urls.username)                                               as users,
--        array_agg(DISTINCT twitter_id)                                         as tweet_ids,
--        array_agg(created_at)                                                  as dates_posted
FROM urls
GROUP BY urls.url
ORDER BY num_tweets DESC
LIMIT 200;

-- Repeated URLs (stripped to hostname) in tweets
WITH urls as (SELECT ti.twitter_id,
                     ti.twitter_author_id,
                     ti.created_at,
                     ti."user" ->> 'username'                       as username,
                     jsonb_path_query(ti.urls, '$[*].url_expanded') as url
              FROM twitter_item ti
              WHERE ti.project_id = 'c5d36b2e-cbb4-47a8-8370-e5f52bb78bf3')
SELECT split_part(url::text, '/', 3)                                          as base_url,
       count(DISTINCT twitter_id)                                             as num_tweets,
       count(DISTINCT url)                                                    as num_urls,
       count(DISTINCT twitter_author_id)                                      as num_users,
       min(created_at)                                                        as first_posted,
       max(created_at)                                                        as last_posted,
       EXTRACT(epoch FROM (max(created_at) - min(created_at))) / 60 / 60 / 24 as lifetime_days,
       justify_interval(max(created_at) - min(created_at))                    as lifetime
--        array_agg(DISTINCT urls.username)                                      as users,
--        array_agg(urls.username)                                               as users,
--        array_agg(DISTINCT twitter_id)                                         as tweet_ids,
--        array_agg(created_at)                                                  as dates_posted
FROM urls
GROUP BY split_part(url::text, '/', 3)
ORDER BY num_tweets DESC
LIMIT 200;


-- Repeated URLs in tweets per technology
WITH tweets as (SELECT DISTINCT ON (twitter_item.twitter_id, ba_tech.value_int) twitter_item.created_at,
                                                                                twitter_item.twitter_author_id,
                                                                                twitter_item.twitter_id,
                                                                                jsonb_array_elements(
                                                                                        CASE
                                                                                            WHEN twitter_item.urls = 'null'
                                                                                                THEN '[
                                                                                              null
                                                                                            ]'::jsonb
                                                                                            ELSE
                                                                                                twitter_item.urls END) ->>
                                                                                'url_expanded'    as url,
                                                                                ba_tech.value_int as technology
                FROM twitter_item
                         LEFT JOIN bot_annotation ba_tech on (
                            twitter_item.item_id = ba_tech.item_id
                        AND ba_tech.bot_annotation_metadata_id = 'fc73da56-9f51-4d2b-ad35-2a01dbe9b275'
                        AND ba_tech.key = 'tech')
                WHERE twitter_item.project_id = 'c5d36b2e-cbb4-47a8-8370-e5f52bb78bf3'
                  AND twitter_item.created_at > '2018-01-01'::timestamp - '5 week'::interval
                  AND twitter_item.created_at < '2018-01-01'::timestamp + '5 week'::interval)
SELECT url,
       count(DISTINCT twitter_id)                                as num_tweets,
       count(DISTINCT twitter_id) FILTER (WHERE technology = 0)  as "Methane Removal",
       count(DISTINCT twitter_id) FILTER (WHERE technology = 1)  as "CCS",
       count(DISTINCT twitter_id) FILTER (WHERE technology = 2)  as "Ocean Fertilization",
       count(DISTINCT twitter_id) FILTER (WHERE technology = 3)  as "Ocean Alkalinization",
       count(DISTINCT twitter_id) FILTER (WHERE technology = 4)  as "Enhanced Weathering",
       count(DISTINCT twitter_id) FILTER (WHERE technology = 5)  as "Biochar",
       count(DISTINCT twitter_id) FILTER (WHERE technology = 6)  as "Afforestation/Reforestation",
       count(DISTINCT twitter_id) FILTER (WHERE technology = 7)  as "Ecosystem Restoration",
       count(DISTINCT twitter_id) FILTER (WHERE technology = 8)  as "Soil Carbon Sequestration",
       count(DISTINCT twitter_id) FILTER (WHERE technology = 9)  as "BECCS",
       count(DISTINCT twitter_id) FILTER (WHERE technology = 10) as "Blue Carbon",
       count(DISTINCT twitter_id) FILTER (WHERE technology = 11) as "Direct Air Capture",
       count(DISTINCT twitter_id) FILTER (WHERE technology = 12) as "GGR (general)"
FROM tweets
GROUP BY url
ORDER BY num_tweets DESC
LIMIT 50;

-- Repeated URLs (stripped to hostname) in tweets per technology
WITH tweets as (SELECT DISTINCT ON (twitter_item.twitter_id, ba_tech.value_int) twitter_item.created_at,
                                                                                twitter_item.twitter_author_id,
                                                                                twitter_item.twitter_id,
                                                                                jsonb_array_elements(
                                                                                        CASE
                                                                                            WHEN twitter_item.urls = 'null'
                                                                                                THEN '[
                                                                                              null
                                                                                            ]'::jsonb
                                                                                            ELSE
                                                                                                twitter_item.urls END) ->>
                                                                                'url_expanded'    as url,
                                                                                ba_tech.value_int as technology
                FROM twitter_item
                         LEFT JOIN bot_annotation ba_tech on (
                            twitter_item.item_id = ba_tech.item_id
                        AND ba_tech.bot_annotation_metadata_id = 'fc73da56-9f51-4d2b-ad35-2a01dbe9b275'
                        AND ba_tech.key = 'tech')
                WHERE twitter_item.project_id = 'c5d36b2e-cbb4-47a8-8370-e5f52bb78bf3'
                  AND twitter_item.created_at > '2018-01-01'::timestamp - '5 week'::interval
                  AND twitter_item.created_at < '2018-01-01'::timestamp + '5 week'::interval)
SELECT url,
       count(DISTINCT twitter_id)                                as num_tweets,
       count(DISTINCT twitter_id) FILTER (WHERE technology = 0)  as "Methane Removal",
       count(DISTINCT twitter_id) FILTER (WHERE technology = 1)  as "CCS",
       count(DISTINCT twitter_id) FILTER (WHERE technology = 2)  as "Ocean Fertilization",
       count(DISTINCT twitter_id) FILTER (WHERE technology = 3)  as "Ocean Alkalinization",
       count(DISTINCT twitter_id) FILTER (WHERE technology = 4)  as "Enhanced Weathering",
       count(DISTINCT twitter_id) FILTER (WHERE technology = 5)  as "Biochar",
       count(DISTINCT twitter_id) FILTER (WHERE technology = 6)  as "Afforestation/Reforestation",
       count(DISTINCT twitter_id) FILTER (WHERE technology = 7)  as "Ecosystem Restoration",
       count(DISTINCT twitter_id) FILTER (WHERE technology = 8)  as "Soil Carbon Sequestration",
       count(DISTINCT twitter_id) FILTER (WHERE technology = 9)  as "BECCS",
       count(DISTINCT twitter_id) FILTER (WHERE technology = 10) as "Blue Carbon",
       count(DISTINCT twitter_id) FILTER (WHERE technology = 11) as "Direct Air Capture",
       count(DISTINCT twitter_id) FILTER (WHERE technology = 12) as "GGR (general)"
FROM tweets
GROUP BY url
ORDER BY num_tweets DESC
LIMIT 50;


-- Cumulative user and tweet numbers
WITH buckets as (SELECT generate_series('2010-01-01 00:00'::timestamp,
                                        '2022-12-31 23:59'::timestamp,
                                        '1 week') as bucket)
SELECT b.bucket                                                                           as bucket,
       count(DISTINCT ti.twitter_id)                                                      as cum_tweets,
       count(DISTINCT ti.twitter_author_id)                                               as cum_users,
       count(DISTINCT ti.twitter_id)::float / count(DISTINCT ti.twitter_author_id)::float as tpu
FROM buckets b
         LEFT JOIN twitter_item ti ON (ti.created_at < b.bucket)
GROUP BY bucket
ORDER BY bucket;

-- Cumulative user and tweet numbers
WITH buckets as (SELECT generate_series('2010-01-01 00:00'::timestamp,
                                        '2022-12-31 23:59'::timestamp,
                                        '1 month') as bucket)
SELECT b.bucket                                                                           as bucket,
       count(DISTINCT ti.twitter_id)                                                      as cum_tweets,
       count(DISTINCT ti.twitter_author_id)                                               as cum_users,
       count(DISTINCT ti.twitter_id)::float / count(DISTINCT ti.twitter_author_id)::float as tpu
FROM buckets b
         LEFT JOIN twitter_item ti ON (
            ti.project_id = 'c5d36b2e-cbb4-47a8-8370-e5f52bb78bf3'
        AND ti.created_at < b.bucket)
GROUP BY bucket
ORDER BY bucket;


-- User and tweet numbers with sliding window
WITH buckets as (SELECT generate_series('2010-01-01 00:00'::timestamp,
                                        '2022-12-31 23:59'::timestamp,
                                        '1 week') as bucket)
SELECT b.bucket                                                                           as bucket,
       count(DISTINCT ti.twitter_id)                                                      as cum_tweets,
       count(DISTINCT ti.twitter_author_id)                                               as cum_users,
       count(DISTINCT ti.twitter_id)::float / count(DISTINCT ti.twitter_author_id)::float as tpu
FROM buckets b
         LEFT JOIN twitter_item ti ON (
            ti.project_id = 'c5d36b2e-cbb4-47a8-8370-e5f52bb78bf3'
        AND ti.created_at >= (b.bucket - '6 months'::interval)
        AND ti.created_at < b.bucket)
WHERE ti.project_id = 'c5d36b2e-cbb4-47a8-8370-e5f52bb78bf3'
GROUP BY bucket
ORDER BY bucket;


-- Cumulative user and tweet numbers for a technology
WITH buckets as (SELECT generate_series('2010-01-01 00:00'::timestamp,
                                        '2022-12-31 23:59'::timestamp,
                                        '3 months') as bucket),
     labels as (SELECT DISTINCT ON (twitter_item.twitter_id, ba_tech.value_int) twitter_item.created_at,
                                                                                twitter_item.twitter_author_id,
                                                                                twitter_item.twitter_id,
                                                                                ba_tech.value_int as technology
                FROM twitter_item
                         LEFT OUTER JOIN bot_annotation ba_tech on (
                            twitter_item.item_id = ba_tech.item_id
                        AND ba_tech.bot_annotation_metadata_id = 'fc73da56-9f51-4d2b-ad35-2a01dbe9b275'
                        AND ba_tech.key = 'tech')
                WHERE twitter_item.project_id = 'c5d36b2e-cbb4-47a8-8370-e5f52bb78bf3'
                  AND ba_tech.value_int = 1)
SELECT b.bucket                                                                         as bucket,
       count(DISTINCT l.twitter_id)                                                     as cum_tweets,
       count(DISTINCT l.twitter_author_id)                                              as cum_users,
       count(DISTINCT l.twitter_id)::float / count(DISTINCT l.twitter_author_id)::float as tpu
FROM buckets b
         LEFT JOIN labels l ON (l.created_at < b.bucket)
GROUP BY bucket
ORDER BY bucket;


-- Cumulative user and tweet numbers within sliding window for a technology
WITH buckets as (SELECT generate_series('2010-01-01 00:00'::timestamp,
                                        '2022-12-31 23:59'::timestamp,
                                        '3 months') as bucket),
     labels as (SELECT DISTINCT ON (twitter_item.twitter_id, ba_tech.value_int) twitter_item.created_at,
                                                                                twitter_item.twitter_author_id,
                                                                                twitter_item.twitter_id,
                                                                                ba_tech.value_int as technology
                FROM twitter_item
                         LEFT OUTER JOIN bot_annotation ba_tech on (
                            twitter_item.item_id = ba_tech.item_id
                        AND ba_tech.bot_annotation_metadata_id = 'fc73da56-9f51-4d2b-ad35-2a01dbe9b275'
                        AND ba_tech.key = 'tech')
                WHERE twitter_item.project_id = 'c5d36b2e-cbb4-47a8-8370-e5f52bb78bf3'
                  AND ba_tech.value_int = 1)
SELECT b.bucket                                                                         as bucket,
       count(DISTINCT l.twitter_id)                                                     as cum_tweets,
       count(DISTINCT l.twitter_author_id)                                              as cum_users,
       count(DISTINCT l.twitter_id)::float / count(DISTINCT l.twitter_author_id)::float as tpu
FROM buckets b
         LEFT JOIN labels l ON (
            l.created_at >= (b.bucket - '6 months'::interval)
        AND l.created_at < b.bucket)
GROUP BY bucket
ORDER BY bucket;


-- Hashtags within a certain time window
WITH labels as (SELECT DISTINCT ON (twitter_item.twitter_id, ba_tech.value_int) twitter_item.created_at,
                                                                                twitter_item.twitter_author_id,
                                                                                twitter_item.twitter_id,
                                                                                jsonb_array_elements(
                                                                                        CASE
                                                                                            WHEN twitter_item.hashtags = 'null'
                                                                                                THEN '[
                                                                                              null
                                                                                            ]'::jsonb
                                                                                            ELSE
                                                                                                twitter_item.hashtags END) ->>
                                                                                'tag'             as tag,
                                                                                ba_tech.value_int as technology
                FROM twitter_item
                         LEFT JOIN bot_annotation ba_tech on (
                            twitter_item.item_id = ba_tech.item_id
                        AND ba_tech.bot_annotation_metadata_id = 'fc73da56-9f51-4d2b-ad35-2a01dbe9b275'
                        AND ba_tech.key = 'tech')
                WHERE twitter_item.project_id = 'c5d36b2e-cbb4-47a8-8370-e5f52bb78bf3'
--                   AND ba_tech.value_int = 1
                  AND twitter_item.created_at > '2018-01-01'::timestamp - '5 week'::interval
                  AND twitter_item.created_at < '2018-01-01'::timestamp + '5 week'::interval)
SELECT tag,
       count(DISTINCT twitter_id)                                as num_tweets,
       count(DISTINCT twitter_id) FILTER (WHERE technology = 0)  as "Methane Removal",
       count(DISTINCT twitter_id) FILTER (WHERE technology = 1)  as "CCS",
       count(DISTINCT twitter_id) FILTER (WHERE technology = 2)  as "Ocean Fertilization",
       count(DISTINCT twitter_id) FILTER (WHERE technology = 3)  as "Ocean Alkalinization",
       count(DISTINCT twitter_id) FILTER (WHERE technology = 4)  as "Enhanced Weathering",
       count(DISTINCT twitter_id) FILTER (WHERE technology = 5)  as "Biochar",
       count(DISTINCT twitter_id) FILTER (WHERE technology = 6)  as "Afforestation/Reforestation",
       count(DISTINCT twitter_id) FILTER (WHERE technology = 7)  as "Ecosystem Restoration",
       count(DISTINCT twitter_id) FILTER (WHERE technology = 8)  as "Soil Carbon Sequestration",
       count(DISTINCT twitter_id) FILTER (WHERE technology = 9)  as "BECCS",
       count(DISTINCT twitter_id) FILTER (WHERE technology = 10) as "Blue Carbon",
       count(DISTINCT twitter_id) FILTER (WHERE technology = 11) as "Direct Air Capture",
       count(DISTINCT twitter_id) FILTER (WHERE technology = 12) as "GGR (general)"
FROM labels
GROUP BY tag
ORDER BY num_tweets DESC
LIMIT 50;


-- Select tweets with a specific hashtag
SELECT i.text, ti.*
FROM twitter_item ti
         LEFT JOIN item i on i.item_id = ti.item_id
WHERE ti.hashtags @> '[{"tag": "geoingenieria"}]'
LIMIT 10;


SELECT distinct on (ti.twitter_id) ti.created_at,
                                   ti.twitter_id                                              as twitter_id,
                                   ti.hashtags                                                as hashtags,
                                   i.text                                                     as text,
                                   array_agg(ba.value_int) OVER ( PARTITION BY ti.twitter_id) as technology
FROM twitter_item ti
         LEFT JOIN item i on i.item_id = ti.item_id
         LEFT JOIN bot_annotation ba on ti.item_id = ba.item_id
WHERE ti.project_id = 'c5d36b2e-cbb4-47a8-8370-e5f52bb78bf3'
  AND ba.bot_annotation_metadata_id = 'fc73da56-9f51-4d2b-ad35-2a01dbe9b275'
  AND ba.key = 'tech'
  AND ti.hashtags @> '[{"tag": "CCS"}]'
LIMIT 50;