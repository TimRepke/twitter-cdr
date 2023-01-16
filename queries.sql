-- Project ID: e63da0c9-9bb5-4026-ab5e-7d5845cdc111

-- Query annotation scheme: d83a736a-791f-4611-9c44-002cb4056202
-- Query bot annotations: fc73da56-9f51-4d2b-ad35-2a01dbe9b275

-- Sentiment and emotion annotation scheme: 8d5d899f-b615-4962-b468-d95893df0921
-- Sentiment bot annotations: e63da0c9-9bb5-4026-ab5e-7d5845cdc111


-- Number of tweets primarily classified as negative(0), neutral(1), positive(2)
SELECT value_int, count(1)
FROM bot_annotation
WHERE key = 'senti'
  AND repeat = 1
  AND bot_annotation_metadata_id = 'e63da0c9-9bb5-4026-ab5e-7d5845cdc111'
GROUP BY value_int;

-- Temporal histogram (1-day resolution) of tweet counts
WITH buckets as (SELECT generate_series('2006-01-01 00:00'::timestamp,
                                        '2022-12-31 23:59'::timestamp,
                                        '1 day') as bucket)
SELECT b.bucket,
       count(ti.item_id)
FROM buckets b
         LEFT JOIN twitter_item ti ON (ti.created_at >= b.bucket and ti.created_at < b.bucket + interval '1 day')
WHERE ti.project_id = 'c5d36b2e-cbb4-47a8-8370-e5f52bb78bf3'
GROUP BY b.bucket
ORDER BY b.bucket;
