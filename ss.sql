WITH page_sessions AS (
  SELECT
    (SELECT value.string_value FROM UNNEST(event_params) WHERE event_name = 'page_view' AND key = 'page_location') AS page_location,
    CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)) AS session_id
  FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE
    _TABLE_SUFFIX BETWEEN '20201101' AND '20201110'
    AND (SELECT value.string_value FROM UNNEST(event_params) WHERE event_name = 'page_view' AND key = 'page_location') IS NOT NULL
)

SELECT
  page_location,
  COUNT(DISTINCT session_id) AS sessions
FROM
  page_sessions
WHERE
  page_location IS NOT NULL
GROUP BY
  page_location
ORDER BY
  sessions DESC
LIMIT 10;
