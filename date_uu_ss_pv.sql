WITH
  AggregatedData AS (
  SELECT
    event_date,
    COUNT(DISTINCT user_pseudo_id) AS users,
    COUNTIF(event_name = 'session_start') AS sessions,
    COUNTIF(event_name = 'page_view') AS pageviews
  FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE
    _TABLE_SUFFIX BETWEEN '20201101'
    AND '20201110'
  GROUP BY
    event_date )
SELECT
  event_date AS date,
  users,
  COALESCE(sessions, 0) AS sessions,
  COALESCE(pageviews, 0) AS pageviews
FROM
  AggregatedData
ORDER BY
  date ASC;