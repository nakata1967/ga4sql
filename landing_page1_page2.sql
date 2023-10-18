/*
このクエリは、まずセッションごとのページ遷移をリスト化し、次にリストから上位のページ遷移パターンを特定します。
そして、そのパターンに基づいてランディングページ、ページA、ページBの順でページ遷移が行われたセッション数を集計します。
*/

WITH
  SessionPageViews AS (
  SELECT
    user_pseudo_id,
    ARRAY_AGG(page_location
    ORDER BY
      event_timestamp ASC) AS pages,
    (
    SELECT
      value.int_value
    FROM
      UNNEST(event_params)
    WHERE
      KEY = 'ga_session_id') AS session_id
  FROM (
    SELECT
      user_pseudo_id,
      event_timestamp,
      (
      SELECT
        value.string_value
      FROM
        UNNEST(event_params)
      WHERE
        KEY = 'page_location') AS page_location,
      event_params
    FROM
      `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
    WHERE
      _TABLE_SUFFIX BETWEEN '20201101'
      AND '20201110'
      AND event_name = 'page_view' )
  GROUP BY
    user_pseudo_id,
    session_id ),
  Patterns AS (
  SELECT
    user_pseudo_id,
    session_id,
    pages[SAFE_OFFSET(0)] AS landing_page,
    pages[SAFE_OFFSET(1)] AS page_a,
    pages[SAFE_OFFSET(2)] AS page_b
  FROM
    SessionPageViews
  WHERE
    ARRAY_LENGTH(pages) >= 3 )
SELECT
  landing_page,
  page_a,
  page_b,
  COUNT(DISTINCT CONCAT(user_pseudo_id, CAST(session_id AS STRING))) AS sessions
FROM
  Patterns
GROUP BY
  landing_page,
  page_a,
  page_b
ORDER BY
  sessions DESC
LIMIT
  10;