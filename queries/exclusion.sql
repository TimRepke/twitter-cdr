WITH user_tweets as (SELECT ti.item_id,
                            ti.twitter_id,
                            ti.twitter_author_id,
                            ti.created_at                 as tweet_timestamp,
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
                     WHERE ti.project_id = 'c5d36b2e-cbb4-47a8-8370-e5f52bb78bf3'
                       AND ti.created_at >= '2010-01-01 00:00'::timestamp
                       AND ti.created_at <= '2022-12-31 23:59'::timestamp)
SELECT ut.twitter_author_id,
       ut.username,
       -- Number of tweets matching any CDR query
       count(DISTINCT ut.twitter_id)                                                  as num_cdr_tweets,
       -- Number of tweets matching any CDR query (excluding Methane Removal (0) and CCS (1) )
       count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int > 1)                  as num_cdr_tweets_noccs,

       -- Tweets that are actually written and not just retweeted or quoted
       count(DISTINCT ut.twitter_id) FILTER ( WHERE ut.is_orig )                      as num_orig_cdr_tweets,
       -- Tweets that are actually written and not just retweeted or quoted (excluding Methane Removal (0) and CCS (1) )
       count(DISTINCT ut.twitter_id) FILTER ( WHERE ut.is_orig AND ba.value_int > 1 ) as num_orig_cdr_tweets_noccs,
       -- Total number of tweets by the user (as per Twitters profile information)
       ut.tweet_count                                                                 as num_tweets,
       (count(DISTINCT ut.twitter_id) FILTER ( WHERE ut.is_orig ))::float /
       count(DISTINCT ut.twitter_id)::float * 100                                     as perc_orig,
       count(DISTINCT ut.twitter_id)::float / ut.tweet_count::float * 100             as perc_cdr,
       count(DISTINCT ut.twitter_id) FILTER (WHERE ba_senti.value_int = 2)            as "Positive",
       count(DISTINCT ut.twitter_id) FILTER (WHERE ba_senti.value_int = 1)            as "Neutral",
       count(DISTINCT ut.twitter_id) FILTER (WHERE ba_senti.value_int = 0)            as "Negative",
       count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 0)                  as "Methane Removal",
       count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 1)                  as "CCS",
       count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 2)                  as "Ocean Fertilization",
       count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 3)                  as "Ocean Alkalinization",
       count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 4)                  as "Enhanced Weathering",
       count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 5)                  as "Biochar",
       count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 6)                  as "Afforestation/Reforestation",
       count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 7)                  as "Ecosystem Restoration",
       count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 8)                  as "Soil Carbon Sequestration",
       count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 9)                  as "BECCS",
       count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 10)                 as "Blue Carbon",
       count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 11)                 as "Direct Air Capture",
       count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int = 12)                 as "GGR (general)",
       ut.tweet_count,
       ut.listed_count,
       ut.followers_count,
       ut.following_count,
       ut.name,
       ut.location,
       min(ut.tweet_timestamp)                                                        as earliest_cdr_tweet,
       max(ut.tweet_timestamp)                                                        as latest_cdr_tweet,
       max(ut.tweet_timestamp) - min(ut.tweet_timestamp)                              as time_cdr_active,
       min(ut.tweet_timestamp) - ut.created_at                                        as time_to_first_cdr,
       ut.created_at,
       ut.verified,
       ut.description
FROM user_tweets ut
         LEFT JOIN bot_annotation ba_senti ON (
            ut.item_id = ba_senti.item_id
        AND ba_senti.bot_annotation_metadata_id = 'e63da0c9-9bb5-4026-ab5e-7d5845cdc111'
        AND ba_senti.key = 'senti'
        AND ba_senti.repeat = 1)
         LEFT JOIN bot_annotation ba ON (
            ut.item_id = ba.item_id
        AND ba.bot_annotation_metadata_id = 'fc73da56-9f51-4d2b-ad35-2a01dbe9b275'
        AND ba.key = 'tech'
    )
GROUP BY ut.name, ut.username, ut.location, ut.tweet_count, ut.listed_count, ut.followers_count,
         ut.following_count,
         ut.created_at, ut.verified, ut.description, ut.twitter_author_id;


WITH buckets as (SELECT generate_series('2006-01-01 00:00'::timestamp,
                                        '2022-12-31 23:59'::timestamp,
                                        '1 year') as bucket),
     tweets as (SELECT ut.twitter_id, ut.twitter_author_id, created_at
                FROM twitter_item ut
                         LEFT JOIN bot_annotation ba_senti ON (
                            ut.item_id = ba_senti.item_id
                        AND ba_senti.bot_annotation_metadata_id = 'e63da0c9-9bb5-4026-ab5e-7d5845cdc111'
                        AND ba_senti.key = 'senti'
                        AND ba_senti.repeat = 1)
                         LEFT JOIN bot_annotation ba_tech ON (
                            ut.item_id = ba_tech.item_id
                        AND ba_tech.bot_annotation_metadata_id = 'fc73da56-9f51-4d2b-ad35-2a01dbe9b275'
                        AND ba_tech.key = 'tech'
                        AND ba_tech.key > 1)
                WHERE project_id = 'c5d36b2e-cbb4-47a8-8370-e5f52bb78bf3'),
     users as (SELECT unnest(array ['14717515','573965125']) as author_id)
SELECT b.bucket,
       count(DISTINCT ti.twitter_id)                                                     as num_tweets_all,
       count(DISTINCT ti.twitter_id) FILTER ( WHERE ti.twitter_author_id = u.author_id ) as num_tweets
FROM buckets b
         LEFT OUTER JOIN tweets ti ON (
            ti.created_at >= (b.bucket - '1 year'::interval)
        AND ti.created_at < b.bucket)
         LEFT OUTER JOIN users u ON ti.twitter_author_id = u.author_id
GROUP BY b.bucket;