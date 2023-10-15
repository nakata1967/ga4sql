WITH
  LandingPages AS (
  SELECT
    event_bundle_sequence_id,
    MAX(CASE
        WHEN event_params.key = 'page_location' THEN event_params.value.string_value
    END
      ) AS page_location,
    MAX(CASE
        WHEN event_params.key = 'page_title' THEN event_params.value.string_value
    END
      ) AS page_title
  FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
    UNNEST(event_params) AS event_params
  WHERE
    _TABLE_SUFFIX BETWEEN '20201101'
    AND '20201110'
    AND event_name = 'page_view'
    AND traffic_source.medium = 'organic'
  GROUP BY
    event_bundle_sequence_id )
SELECT
  page_location,
  page_title,
  COUNT(*) AS search_traffic_count
FROM
  LandingPages
WHERE
  page_location IS NOT NULL
  AND page_title IS NOT NULL
GROUP BY
  page_location,
  page_title
ORDER BY
  search_traffic_count DESC
LIMIT
  10;