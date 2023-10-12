WITH
  PurchaseSequence AS (
  SELECT
    user_pseudo_id,
    param.value.int_value AS session_id,
    MIN(event_timestamp) AS first_purchase_time
  FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
    UNNEST(event_params) AS param
  WHERE
    _TABLE_SUFFIX BETWEEN '20201101'
    AND '20201110'
    AND event_name = 'purchase'
    AND param.key = 'ga_session_id'
  GROUP BY
    user_pseudo_id,
    param.value.int_value ),
  ViewsBeforePurchase AS (
  SELECT
    DISTINCT e.user_pseudo_id,
    session_param.value.int_value AS session_id,
    page_param.value.string_value AS page_location
  FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` e,
    UNNEST(e.event_params) AS page_param,
    UNNEST(e.event_params) AS session_param
  WHERE
    e._TABLE_SUFFIX BETWEEN '20201101'
    AND '20201110'
    AND page_param.key = 'page_location'
    AND session_param.key = 'ga_session_id' ),
  FilteredViews AS (
  SELECT
    v.user_pseudo_id,
    v.session_id,
    v.page_location
  FROM
    ViewsBeforePurchase v
  JOIN
    PurchaseSequence p
  ON
    v.user_pseudo_id = p.user_pseudo_id
    AND v.session_id = p.session_id
  WHERE
    v.page_location IS NOT NULL )
SELECT
  page_location,
  COUNT(DISTINCT CONCAT(user_pseudo_id, '_', CAST(session_id AS STRING))) AS unique_sessions
FROM
  FilteredViews
GROUP BY
  page_location
ORDER BY
  unique_sessions DESC
LIMIT
  20;