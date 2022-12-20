import logging
from datetime import datetime
from typing import Generator, Any
from searchtweets import ResultStream
from common.config import settings

from nacsos_data.models.items.twitter import TwitterItemModel, TwitterUserModel, Hashtag, Cashtag, ContextAnnotation, \
    Mention, URL, ReferencedTweet


def api_page_to_tweets(page: dict[str, Any]) -> Generator[TwitterItemModel, None, None]:
    """
    This function takes the dictionary (JSON response) from the Twitter search API
    and yields consolidated `TwitterItemModel`s.
    """
    users = {}
    if 'includes' in page and 'users' in page['includes']:
        users = {user['id']: user for user in page['includes']['users']}

    for tweet in page['data']:
        user = None
        if tweet['author_id'] in users:
            user_obj = users[tweet['author_id']]
            user = TwitterUserModel(
                created_at=datetime.strptime(user_obj['created_at'][:19], '%Y-%m-%dT%H:%M:%S'),
                name=None if user_obj['name'] == user_obj['username'] else user_obj['name'],
                username=user_obj['username'],
                verified=user_obj['verified'],
                description=user_obj.get('description'),
                location=user_obj.get('location'),
                followers_count=user_obj.get('public_metrics', {}).get('followers_count'),
                following_count=user_obj.get('public_metrics', {}).get('following_count'),
                tweet_count=user_obj.get('public_metrics', {}).get('tweet_count'),
                listed_count=user_obj.get('public_metrics', {}).get('listed_count')
            ).dict(exclude_none=True)

        ref_tweets = None
        if 'referenced_tweets' in tweet:
            ref_tweets = [ReferencedTweet(id=ref_tweet['id'], type=ref_tweet['type'])
                          for ref_tweet in tweet['referenced_tweets']]
        latlon = None
        if 'geo' in tweet and 'coordinates' in tweet['geo'] and tweet['geo'].get('type') == 'Point':
            latlon = tweet['geo']['coordinates'].get('coordinates')

        hashtags = None
        if 'entities' in tweet and 'hashtags' in tweet['entities']:
            hashtags = [Hashtag(start=ht['start'], end=ht['end'], tag=ht['tag'])
                        for ht in tweet['entities']['hashtags']]
        cashtags = None
        if 'entities' in tweet and 'cashtags' in tweet['entities']:
            cashtags = [Cashtag(start=ct['start'], end=ct['end'], tag=ct['tag'])
                        for ct in tweet['entities']['cashtags']]
        urls = None
        if 'entities' in tweet and 'urls' in tweet['entities']:
            urls = [URL(start=url['start'], end=url['end'], url=url['url'], url_expanded=url['expanded_url'])
                    for url in tweet['entities']['urls']]
        mentions = None
        if 'entities' in tweet and 'mentions' in tweet['entities']:
            mentions = [Mention(start=m['start'], end=m['end'], username=m['username'], user_id=m['id'])
                        for m in tweet['entities']['mentions']]

        annotations = None
        if 'context_annotations' in tweet:
            annotations = [ContextAnnotation(domain_id=ca['domain']['id'], domain_name=ca['domain']['name'],
                                             entity_id=ca['entity']['id'], entity_name=ca['entity']['name'])
                           for ca in tweet['context_annotations']]

        tweet_obj = TwitterItemModel(
            twitter_id=str(tweet['id']),
            twitter_author_id=str(tweet['author_id']),
            text=tweet['text'],
            created_at=datetime.strptime(tweet['created_at'][:19], '%Y-%m-%dT%H:%M:%S'),
            language=tweet.get('lang'),
            conversation_id=tweet.get('conversation_id'),
            referenced_tweets=ref_tweets,
            annotations=annotations,
            latitude=latlon[0] if latlon else None,
            longitude=latlon[1] if latlon else None,
            hashtags=hashtags,
            mentions=mentions,
            urls=urls,
            cashtags=cashtags,
            retweet_count=tweet['public_metrics']['retweet_count'],
            reply_count=tweet['public_metrics']['reply_count'],
            like_count=tweet['public_metrics']['like_count'],
            quote_count=tweet['public_metrics']['quote_count'],
            user=user  # type: ignore[arg-type]
        )

        yield tweet_obj


def download_query(query: str) -> Generator[TwitterItemModel, None, None]:
    request_params = {
        'query': query,
        'tweet.fields': 'attachments,author_id,conversation_id,created_at,entities,geo,id,'
                        'in_reply_to_user_id,lang,possibly_sensitive,public_metrics,referenced_tweets,reply_settings,'
                        'source,text,withheld,context_annotations',
        'expansions': 'attachments.media_keys,attachments.poll_ids,author_id,entities.mentions.username,'
                      'geo.place_id,in_reply_to_user_id',
        # additional expansions: referenced_tweets.id,referenced_tweets.id.author_id
        'media.fields': 'alt_text,duration_ms,height,media_key,preview_image_url,public_metrics,type,url,variants,width',
        'poll.fields': 'duration_minutes,end_datetime,id,options,voting_status',
        'user.fields': 'created_at,description,entities,id,location,name,pinned_tweet_id,profile_image_url,'
                       'protected,public_metrics,url,username,verified,withheld',
        'place.fields': 'contained_within,country,country_code,full_name,geo,id,name,place_type',
        'sort_order': 'recency',
        'start_time': '2006-03-21T00:00:00Z',
        # 'end_time': '2022-12-31T23:59:59Z', # FIXME
        'end_time': '2022-12-19T23:59:59Z',
        'max_results': 100
    }

    logging.info(f'Starting stream for query: {query}')
    logging.debug(f'Bearer: {settings.TWITTER_BEARER}')
    stream = ResultStream(
        endpoint='https://api.twitter.com/2/tweets/search/all',
        request_parameters=request_params,
        bearer_token=settings.TWITTER_BEARER,
        max_tweets=10 ** 15,
        max_requests=10 ** 9,
        output_format='r')

    for results in stream.stream():
        if 'data' in results and type(results['data']) == list:
            logging.debug(f'Received page with {len(results["data"])} tweets!')
            yield from api_page_to_tweets(results)
        else:
            logging.error('Something went wrong!')
