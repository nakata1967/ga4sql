SELECT
  user_pseudo_id,
  COUNT(*) AS number_of_sessions
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
WHERE
  _TABLE_SUFFIX BETWEEN '20201101'
  AND '20201110'
  AND event_name = 'session_start'
GROUP BY
  user_pseudo_id
ORDER BY
  number_of_sessions DESC
LIMIT
  10;