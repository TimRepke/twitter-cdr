import pickle

from sqlalchemy import text
from shared.models import UserTweetCounts, senti_fields, tech_fields, TechnologyCounts, SentimentCounts
from shared.db import run_query
from shared.config import settings


def fetch_user_stats() -> list[UserTweetCounts]:
    query = text("""
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
                                 WHERE ti.project_id = :project_id
                                   AND ti.created_at >= :start ::timestamp
                                   AND ti.created_at <= :end ::timestamp)
            SELECT ut.twitter_author_id,
                   MAX(ut.username)                                                               as username,
                   -- Number of tweets matching any CDR query
                   count(DISTINCT ut.twitter_id)                                                  as num_cdr_tweets,
                   -- Number of tweets matching any CDR query (excluding Methane Removal (0) and CCS (1) )
                   count(DISTINCT ut.twitter_id) FILTER (WHERE ba.value_int > 1)                  as num_cdr_tweets_noccs,

                   -- Tweets that are actually written and not just retweeted or quoted
                   count(DISTINCT ut.twitter_id) FILTER ( WHERE ut.is_orig )                      as num_orig_cdr_tweets,
                   -- Tweets that are actually written and not just retweeted or quoted (excluding Methane Removal (0) and CCS (1) )
                   count(DISTINCT ut.twitter_id) FILTER ( WHERE ut.is_orig AND ba.value_int > 1 ) as num_orig_cdr_tweets_noccs,
                   -- Total number of tweets by the user (as per Twitters profile information)
                   MAX(ut.tweet_count)                                                            as num_tweets,
                   (count(DISTINCT ut.twitter_id) FILTER ( WHERE ut.is_orig ))::float /
                   count(DISTINCT ut.twitter_id)::float * 100                                     as perc_orig,
                   count(DISTINCT ut.twitter_id)::float / MAX(ut.tweet_count)::float * 100        as perc_cdr,
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
                   MAX(ut.tweet_count) as tweet_count,
                   MAX(ut.listed_count) as listed_count,
                   MAX(ut.followers_count) as followers_count,
                   MAX(ut.following_count) as following_count,
                   MAX(ut.name) as name,
                   MAX(ut.location) as location,
                   min(ut.tweet_timestamp)                                                        as earliest_cdr_tweet,
                   max(ut.tweet_timestamp)                                                        as latest_cdr_tweet,
                   max(ut.tweet_timestamp) - min(ut.tweet_timestamp)                              as time_cdr_active,
                   min(ut.tweet_timestamp) - MAX(ut.created_at)                                   as time_to_first_cdr,
                   min(ut.tweet_timestamp) FILTER (WHERE ba.value_int >1)                         as earliest_cdr_tweet_noccs,
                   max(ut.tweet_timestamp) FILTER (WHERE ba.value_int >1)                         as latest_cdr_tweet_noccs,
                   MAX(ut.created_at) as created_at,
                   bool_or(ut.verified) as verified,
                   MAX(ut.description) as description
            FROM user_tweets ut
                     LEFT JOIN bot_annotation ba_senti ON (
                        ut.item_id = ba_senti.item_id
                    AND ba_senti.bot_annotation_metadata_id = :ba_senti
                    AND ba_senti.key = 'senti'
                    AND ba_senti.repeat = 1)
                     LEFT JOIN bot_annotation ba ON (
                        ut.item_id = ba.item_id
                    AND ba.bot_annotation_metadata_id = :ba_tech
                    AND ba.key = 'tech'
                )
            GROUP BY ut.twitter_author_id;
        """)

    def row_to_obj(row) -> UserTweetCounts:
        user_data = {}
        tech_counts = {}
        senti_counts = {}
        for key, value in row.items():
            if key in tech_fields:
                tech_counts[key] = value
            elif key in senti_fields:
                senti_counts[key] = value
            else:
                user_data[key] = value
        return UserTweetCounts(**user_data,
                               technologies=TechnologyCounts(**tech_counts),
                               sentiments=SentimentCounts(**senti_counts))

    result = run_query(query,
                       params={
                           'project_id': settings.PROJECT_ID,
                           'ba_tech': settings.BA_TECH,
                           'ba_senti': settings.BA_SENT,
                           'start': '2006-01-01 00:00',
                           'end': '2022-12-31 23:59'
                       },
                       row2obj=row_to_obj)

    print('Storing user data')
    with open('data/user_stats.pkl', 'wb') as f:
        pickle.dump(result, f)
    return result


if __name__ == '__main__':
    fetch_user_stats()
