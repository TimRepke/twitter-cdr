SELECT bot_annotation_metadata_id, key, value_int, count(1)
FROM bot_annotation
WHERE bot_annotation_metadata_id = Any (
    Array ['e63da0c9-9bb5-4026-ab5e-7d5845cdc111'::uuid, 'fc73da56-9f51-4d2b-ad35-2a01dbe9b275'::uuid])
GROUP BY bot_annotation_metadata_id, value_int, key
ORDER BY bot_annotation_metadata_id, key, value_int;

WITH sentiment(value, name) AS (VALUES (0, 'negative'), (1, 'neutral'), (2, 'positive')),
     techno(value, name) AS (Values (0, 'Methane removal'),
                                    (1, 'CCS'),
                                    (2, 'Ocean Fertilization'),
                                    (3, 'Ocean Alkalinization'),
                                    (4, 'Enhanced Weathering'),
                                    (5, 'Biochar'),
                                    (6, 'Afforestation/Reforestation'),
                                    (7, 'Ecosystem Restoration'),
                                    (8, 'Soil Carbon Sequestration'),
                                    (9, 'BECCS'),
                                    (10, 'Blue Carbon'),
                                    (11, 'Direct Air Capture'),
                                    (12, 'GGR (general)')),
     queries (key, value, query_id, query) AS (VALUES ('cat_0', 0, 'c_18', '"methane direct air capture"'),
                                                      ('cat_0', 1, 'c_19', '"methane capture"'),
                                                      ('cat_0', 2, 'c_54', 'methane removing atmosphere'),
                                                      ('cat_1', 0, 'c_10', '"co2 sequestration" storage'),
                                                      ('cat_1', 1, 'c_11', '"carbon sequestration" storage'),
                                                      ('cat_1', 2, 'c_12', '"carbon dioxide sequestration"'),
                                                      ('cat_1', 3, 'c_13', '"carbon capture" storage'),
                                                      ('cat_1', 4, 'c_14', '"carbon storage" capture'),
                                                      ('cat_1', 5, 'c_15', '"carbon dioxide capture" storage'),
                                                      ('cat_1', 6, 'c_16', '"carbon dioxide storage" capture'),
                                                      ('cat_1', 7, 'c_17', 'CCS (climate OR carbon OR co2)'),
                                                      ('cat_2', 0, 'c_20',
                                                       '"ocean fertilization" OR "ocean fertilisation" '),
                                                      ('cat_2', 1, 'c_21',
                                                       '"iron fertilization" OR "iron fertilisation"'),
                                                      ('cat_2', 2, 'c_22',
                                                       '(fertilization OR fertilisation) (phytoplankton OR algae) (climate OR carbon OR co2)'),
                                                      ('cat_2', 3, 'c_50', '"iron seeding" (climate OR co2 OR carbon)'),
                                                      ('cat_3', 0, 'c_23', '"ocean liming" -from:spangletoes'),
                                                      ('cat_3', 1, 'c_49', '"ocean alkalinity enhancement"'),
                                                      ('cat_3', 2, 'c_57',
                                                       '"ocean alkalinization" OR "ocean alkalinisation"'),
                                                      ('cat_4', 0, 'c_24', '"enhanced weathering" -from:spangletoes'),
                                                      ('cat_4', 1, 'c_25',
                                                       '(olivine OR basalt OR silicate) (co2 OR emission OR emissions)'),
                                                      ('cat_4', 2, 'c_26', 'olivine weathering'),
                                                      ('cat_4', 3, 'c_51',
                                                       '(basalt OR silicate) weathering (co2 OR carbon OR enhanced)'),
                                                      ('cat_5', 0, 'c_27',
                                                       '(biochar OR bio-char) (co2 OR carbon OR climate OR emission OR sequestration OR "greenhouse gas")'),
                                                      ('cat_6', 0, 'c_29',
                                                       'afforestation (climate OR co2 OR emission OR emissions OR  "greenhouse gas"  OR ghg OR carbon)'),
                                                      ('cat_6', 1, 'c_30',
                                                       'reforestation (climate OR co2 OR emission OR emissions OR  "greenhouse gas"  OR ghg OR carbon)'),
                                                      ('cat_6', 2, 'c_31', 'tree planting climate'),
                                                      ('cat_7', 0, 'c_32',
                                                       '(re-wilding OR rewilding) (climate OR carbon OR CO2 OR "greenhouse gas" OR GHG)'),
                                                      ('cat_7', 1, 'c_56',
                                                       '("ecosystem restoration" OR "restore ecosystem")  (climate OR carbon OR CO2 OR "greenhouse gas" OR GHG)'),
                                                      ('cat_8', 0, 'c_33', 'soil sequestration (co2 OR carbon)'),
                                                      ('cat_8', 1, 'c_36', '"soil carbon"'),
                                                      ('cat_8', 2, 'c_37', '"carbon farming"'),
                                                      ('cat_9', 0, 'c_38',
                                                       'BECCS (co2 OR carbon OR climate OR ccs OR biomass OR emission OR emissions)'),
                                                      ('cat_9', 1, 'c_39',
                                                       'biomass ("carbon capture" OR "capture carbon" OR  "co2 capture" OR "capture CO2" OR ccs)'),
                                                      ('cat_9', 2, 'c_40',
                                                       'bioenergy ("carbon capture" OR "capture carbon" OR  "co2 capture" OR "capture CO2" OR ccs)'),
                                                      ('cat_10', 0, 'c_41', 'seagrass (carbon OR co2)'),
                                                      ('cat_10', 1, 'c_42', 'macroalgae (carbon OR co2)'),
                                                      ('cat_10', 2, 'c_43', 'mangrove (carbon OR co2)'),
                                                      ('cat_10', 3, 'c_52', 'kelp (carbon OR co2)'),
                                                      ('cat_10', 4, 'c_53',
                                                       '(wetland OR wetlands OR marsh OR marshes OR peatland OR peatlands OR peat OR bog OR  bogs) (carbon OR co2) (restore OR restoration OR rehabilitation)'),
                                                      ('cat_10', 5, 'c_44', '"blue carbon"'),
                                                      ('cat_11', 0, 'c_45',
                                                       'DAC (climate OR carbon OR co2 OR emission OR emissions)'),
                                                      ('cat_11', 1, 'c_46', '"direct air capture"'),
                                                      ('cat_11', 2, 'c_47',
                                                       '("carbon capture" OR "co2 capture") ("ambient air" OR "direct air")'),
                                                      ('cat_11', 3, 'c_48', 'DACCS (carbon OR co2 OR climate)'),
                                                      ('cat_12', 0, 'c_09', '"methane removal"'),
                                                      ('cat_12', 1, 'c_01', '"negative emissions"'),
                                                      ('cat_12', 2, 'c_02', '"negative emission"'),
                                                      ('cat_12', 3, 'c_03', '"carbon dioxide removal"'),
                                                      ('cat_12', 4, 'c_04',
                                                       '"co2 removal" -submarine -"space station"'),
                                                      ('cat_12', 5, 'c_05', '"carbon removal"'),
                                                      ('cat_12', 6, 'c_06', '"greenhouse gas removal"'),
                                                      ('cat_12', 7, 'c_07', '"ghg removal"'),
                                                      ('cat_12', 8, 'c_08',
                                                       '"carbon negative" (climate OR co2 OR emission OR  "greenhouse gas"  OR ghg)'),
                                                      ('cat_12', 9, 'c_55',
                                                       '(remove OR removing OR removed) (carbon OR co2) atmosphere')),
     tweets AS (SELECT ti.item_id,
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
                  AND ti.created_at >= '2006-01-01'::timestamp
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
                              AND n_cdr_tweets_noccs <= 2 THEN 'A'
                          WHEN n_tweets / days <= 100
                              AND n_cdr_tweets_noccs > 2
                              AND n_cdr_tweets_noccs <= 50 THEN 'B'
                          WHEN n_tweets / days <= 100
                              AND n_cdr_tweets_noccs > 50 THEN 'C'
                          ELSE 'EX'
                          END as panel
               FROM users_pre),
     technology AS (SELECT item_id,
                           array_agg(distinct techno.name) as technologies_str,
                           array_agg(distinct value_int)   as technologies_int
                    FROM bot_annotation
                             JOIN techno ON value_int = techno.value
                    WHERE bot_annotation_metadata_id = 'fc73da56-9f51-4d2b-ad35-2a01dbe9b275'
                      AND key = 'tech'
                    GROUP BY item_id),
     query AS (SELECT item_id,
                      array_agg(distinct q.query_id) as query_ids,
                      array_agg(distinct q.query)    as queries
               FROM bot_annotation ba
                        JOIN queries q ON ba.value_int = q.value AND ba.key = q.key
               WHERE ba.bot_annotation_metadata_id = 'fc73da56-9f51-4d2b-ad35-2a01dbe9b275'
               GROUP BY item_id),
     sent as (SELECT item_id, array_agg(sentiment.name ORDER BY ba2.repeat) as sentiment, array_agg(confidence ORDER BY ba2.repeat) as sentiment_confidence
              FROM bot_annotation ba2
                       JOIN sentiment ON ba2.value_int = value
              WHERE ba2.bot_annotation_metadata_id = 'e63da0c9-9bb5-4026-ab5e-7d5845cdc111'
              GROUP BY item_id              )
SELECT t.item_id,
       t.twitter_id,
       t.created_at,
       u.*,
       tech.technologies_str,
       tech.technologies_int,
       query.queries,
       query.query_ids,
       sent.sentiment,
       sent.sentiment_confidence
FROM twitter_item t
         LEFT OUTER JOIN users u ON u.twitter_author_id = t.twitter_author_id
         LEFT OUTER JOIN technology tech ON tech.item_id = t.item_id
         LEFT OUTER JOIN query ON query.item_id = t.item_id
         LEFT OUTER JOIN sent ON sent.item_id = t.item_id
WHERE t.project_id = 'c5d36b2e-cbb4-47a8-8370-e5f52bb78bf3';

