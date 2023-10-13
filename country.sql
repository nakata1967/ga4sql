WITH
  CountryAggregation AS (
  SELECT
    geo.country AS country,
    COUNT(DISTINCT user_pseudo_id) AS user_count
  FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE
    _TABLE_SUFFIX BETWEEN '20201101'
    AND '20201110'
  GROUP BY
    geo.country ),
  GroupedCountries AS (
  SELECT
    CASE
      WHEN user_count < 10 THEN '(Others)'
    ELSE
    country
  END
    AS grouped_country,
    user_count
  FROM
    CountryAggregation )
SELECT
  grouped_country AS country,
  SUM(user_count) AS total_users
FROM
  GroupedCountries
GROUP BY
  grouped_country
ORDER BY
  total_users DESC;