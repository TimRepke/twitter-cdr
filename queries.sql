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
       count(1)                                                 as num_cdr_tweets,
       -- Tweets that are actually written and not just retweeted or quoted
       count(1) FILTER ( WHERE ti.referenced_tweets = 'null' ) as num_orig_cdr_tweets,
       -- Total number of tweets by the user (as per Twitters profile information)
       u.tweet_count,
       u.listed_count,
       u.followers_count,
       u.following_count,
       u.name,
       u.location,
       min(ti.created_at)                                       as earliest_cdr_tweet,
       max(ti.created_at)                                       as latest_cdr_tweet,
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
LIMIT 20;

