SELECT
  params.value.string_value AS page_url,
  COUNT(event_name) AS page_view
FROM
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
  UNNEST (event_params) AS params
WHERE
  _TABLE_SUFFIX BETWEEN '20201101'
  AND '20201110'
  AND event_name = 'page_view'
  AND params.key = 'page_location'
GROUP BY
  page_url
ORDER BY
  page_view DESC;