WITH
  UserFirstPurchase AS ( -- 各ユーザーごとに初めてのpurchaseが発生したセッション番号を取得
  SELECT
    user_pseudo_id,
    MIN(session_number) AS first_purchase_session_number
  FROM (
    SELECT
      user_pseudo_id,
      event_name,
      (
      SELECT
        value.int_value
      FROM
        UNNEST(event_params)
      WHERE
        KEY = 'ga_session_number') AS session_number
    FROM
      `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
    WHERE
      _TABLE_SUFFIX BETWEEN '20201101'
      AND '20201110' )
  WHERE
    event_name = 'purchase'
  GROUP BY
    user_pseudo_id )
SELECT
  CASE
    WHEN first_purchase_session_number <= 20 THEN CAST(first_purchase_session_number AS STRING)
  ELSE
  '21以上'
END
  AS session_count,
  COUNT(DISTINCT user_pseudo_id) AS user_count
FROM
  UserFirstPurchase
GROUP BY
  session_count
ORDER BY
  CASE
    WHEN session_count = '21以上' THEN 999
  ELSE
  CAST(session_count AS INT64)
END
  ;