import datetime
from typing_extensions import TypedDict

from pydantic import BaseModel

TechnologyCounts = TypedDict('TechnologyCounts', {
    'Methane Removal': int,
    'CCS': int,
    'Ocean Fertilization': int,
    'Ocean Alkalinization': int,
    'Enhanced Weathering': int,
    'Biochar': int,
    'Afforestation/Reforestation': int,
    'Ecosystem Restoration': int,
    'Soil Carbon Sequestration': int,
    'BECCS': int,
    'Blue Carbon': int,
    'Direct Air Capture': int,
    'GGR (general)': int
})

SentimentCounts = TypedDict('SentimentCounts', {
    'Negative': int,
    'Neutral': int,
    'Positive': int
})


class UserTweetCounts(BaseModel):
    twitter_author_id: str
    username: str
    num_cdr_tweets: int
    num_orig_cdr_tweets: int
    num_cdr_tweets_noccs: int
    num_orig_cdr_tweets_noccs: int
    num_tweets: int
    perc_orig: float
    perc_cdr: float
    tweet_count: int
    listed_count: int
    followers_count: int
    following_count: int
    name: str | None
    location: str | None
    earliest_cdr_tweet: datetime.datetime
    latest_cdr_tweet: datetime.datetime
    earliest_cdr_tweet_noccs: datetime.datetime | None
    latest_cdr_tweet_noccs: datetime.datetime | None
    time_cdr_active: datetime.timedelta
    time_to_first_cdr: datetime.timedelta
    created_at: datetime.datetime
    verified: bool
    description: str
    technologies: TechnologyCounts
    sentiments: SentimentCounts


senti_fields = set(SentimentCounts.__annotations__.keys())
tech_fields = set(TechnologyCounts.__annotations__.keys())


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
