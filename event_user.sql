SELECT
  event_name,
  COUNT(DISTINCT user_pseudo_id) AS unique_users
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
WHERE
  _TABLE_SUFFIX BETWEEN '20201101'
  AND '20201110'
GROUP BY
  event_name
ORDER BY
  unique_users DESC
LIMIT
  10;