-- Project ID: e63da0c9-9bb5-4026-ab5e-7d5845cdc111

-- Query annotation scheme: d83a736a-791f-4611-9c44-002cb4056202
-- Query bot annotations: fc73da56-9f51-4d2b-ad35-2a01dbe9b275

-- Sentiment and emotion annotation scheme: 8d5d899f-b615-4962-b468-d95893df0921
-- Sentiment bot annotations: e63da0c9-9bb5-4026-ab5e-7d5845cdc111

-- Number of tweets in the project
SELECT count(1) as num_tweets
FROM twitter_item ti
WHERE ti.project_id = 'c5d36b2e-cbb4-47a8-8370-e5f52bb78bf3';

-- Number of tweets primarily classified as negative(0), neutral(1), positive(2)
SELECT value_int, count(1) as num_tweets
FROM bot_annotation
WHERE key = 'senti'
  AND repeat = 1
  AND bot_annotation_metadata_id = 'e63da0c9-9bb5-4026-ab5e-7d5845cdc111'
GROUP BY value_int;


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
       count(DISTINCT labels.twitter_id) FILTER (WHERE labels.technology = 0)  as "Methane removal",
       count(DISTINCT labels.twitter_id) FILTER (WHERE labels.technology = 1)  as "CCS",
       count(DISTINCT labels.twitter_id) FILTER (WHERE labels.technology = 2)  as "Ocean fertilization",
       count(DISTINCT labels.twitter_id) FILTER (WHERE labels.technology = 3)  as "Ocean alkalinization",
       count(DISTINCT labels.twitter_id) FILTER (WHERE labels.technology = 4)  as "Enhanced weathering",
       count(DISTINCT labels.twitter_id) FILTER (WHERE labels.technology = 5)  as "Biochar",
       count(DISTINCT labels.twitter_id) FILTER (WHERE labels.technology = 6)  as "Afforestation/reforestation",
       count(DISTINCT labels.twitter_id) FILTER (WHERE labels.technology = 7)  as "Ecosystem restoration",
       count(DISTINCT labels.twitter_id) FILTER (WHERE labels.technology = 8)  as "Soil carbon sequestration",
       count(DISTINCT labels.twitter_id) FILTER (WHERE labels.technology = 9)  as "BECCS",
       count(DISTINCT labels.twitter_id) FILTER (WHERE labels.technology = 10) as "Blue carbon",
       count(DISTINCT labels.twitter_id) FILTER (WHERE labels.technology = 11) as "Direct air capture",
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
       count(DISTINCT ut.twitter_id)                                  as num_cdr_tweets,
       -- Tweets that are actually written and not just retweeted or quoted
       count(DISTINCT ut.twitter_id) FILTER ( WHERE ut.is_orig )      as num_orig_cdr_tweets,
       -- Total number of tweets by the user (as per Twitters profile information)
       count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 0)  as "Methane removal",
       count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 1)  as "CCS",
       count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 2)  as "Ocean fertilization",
       count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 3)  as "Ocean alkalinization",
       count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 4)  as "Enhanced weathering",
       count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 5)  as "Biochar",
       count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 6)  as "Afforestation/reforestation",
       count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 7)  as "Ecosystem restoration",
       count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 8)  as "Soil carbon sequestration",
       count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 9)  as "BECCS",
       count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 10) as "Blue carbon",
       count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 11) as "Direct air capture",
       count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 12) as "GGR (general)",
       ut.tweet_count,
       ut.listed_count,
       ut.followers_count,
       ut.following_count,
       ut.name,
       ut.location,
       min(ut.created_at)                                             as earliest_cdr_tweet,
       max(ut.created_at)                                             as latest_cdr_tweet,
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