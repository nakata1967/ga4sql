/*
クエリ内の直帰率の計算方法

クエリ内でのbounce_rateの計算式は以下のようになっています：

ROUND(100 * (1 - SAFE_DIVIDE(SUM(CAST(engaged_sessions AS INT)), COUNT(DISTINCT session_id))), 1)

この式をステップバイステップで分解してみましょう。

ステップ1: engaged_sessions

この部分では、セッションが「engaged」であったかどうかを示す値を合計しています。

ステップ2: セッション数のカウント
次に、COUNT(DISTINCT session_id)で全セッションの数をカウントしています。

ステップ3: 直帰率の計算
次に、1 - SAFE_DIVIDE(合計engaged_sessions, 全セッション数)という式で、直帰したセッションの割合を計算しています。

具体的には、全セッション数からengagedだったセッションの数を引き、それを全セッション数で割ることで、直帰率を計算しています。

ステップ4: パーセンテージ表示
最後に、結果を100倍してパーセンテージとして表示しています。そして、ROUND( ..., 1)を使用して小数点以下を1桁に丸めています。
*/

WITH
  SourceMedium AS (
  SELECT
    IFNULL( CONCAT( MAX(CASE
            WHEN ( SELECT value.int_value FROM UNNEST(event_params) WHERE KEY = 'entrances' AND event_name = 'page_view') = 1 AND ( SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'source' AND event_name = 'page_view') IS NULL THEN traffic_source.source
          ELSE
          (
          SELECT
            value.string_value
          FROM
            UNNEST(event_params)
          WHERE
            KEY = 'source')
        END
          ), ' / ', MAX(CASE
            WHEN ( SELECT value.int_value FROM UNNEST(event_params) WHERE KEY = 'entrances' AND event_name = 'page_view') = 1 AND ( SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'medium' AND event_name = 'page_view') IS NULL THEN traffic_source.medium
          ELSE
          (
          SELECT
            value.string_value
          FROM
            UNNEST(event_params)
          WHERE
            KEY = 'medium')
        END
          ) ), CONCAT(traffic_source.source, ' / ', traffic_source.medium) ) AS source_medium,
    CONCAT(user_pseudo_id, CAST(MAX(
        IF
          (event_name='page_view'
            AND (
            SELECT
              value.int_value
            FROM
              UNNEST(event_params)
            WHERE
              KEY = 'ga_session_id') IS NOT NULL, (
            SELECT
              value.int_value
            FROM
              UNNEST(event_params)
            WHERE
              KEY = 'ga_session_id'), NULL)) AS STRING)) AS session_id,
    MAX(
    IF
      (event_name='page_view'
        AND (
        SELECT
          value.string_value
        FROM
          UNNEST(event_params)
        WHERE
          KEY = 'session_engaged') IS NOT NULL, (
        SELECT
          value.string_value
        FROM
          UNNEST(event_params)
        WHERE
          KEY = 'session_engaged'), NULL)) AS engaged_sessions
  FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE
    _TABLE_SUFFIX BETWEEN '20201101'
    AND '20201110'
  GROUP BY
    user_pseudo_id,
    traffic_source.source,
    traffic_source.medium )
SELECT
  source_medium,
  COUNT(DISTINCT session_id) AS sessions,
  CONCAT(FORMAT("%.1f", 100 * (1 - SAFE_DIVIDE(SUM(CAST(engaged_sessions AS INT)), COUNT(DISTINCT session_id)))), '%') AS bounce_rate
FROM
  SourceMedium
GROUP BY
  source_medium
ORDER BY
  sessions DESC
LIMIT
  20;