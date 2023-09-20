WITH
  TotalSessions AS ( -- セッションの総数を集計
  SELECT
    COUNT(DISTINCT event_bundle_sequence_id) AS total_sessions
  FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE
    _TABLE_SUFFIX BETWEEN '20201101'
    AND '20201110'
    AND event_name = 'session_start' ),
  HourlySessions AS ( -- 各時間帯ごとのセッション数を集計
  SELECT
    EXTRACT(HOUR
    FROM
      TIMESTAMP_MICROS(event_timestamp)) AS hour,
    COUNT(DISTINCT event_bundle_sequence_id) AS session_count
  FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE
    _TABLE_SUFFIX BETWEEN '20201101'
    AND '20201110'
    AND event_name = 'session_start'
  GROUP BY
    hour ) -- 各時間帯ごとのセッションの割合を計算
SELECT
  FORMAT('%02d', hour) AS hh,
  FORMAT("%.3f%%", ROUND((session_count * 100.0 / total_sessions), 3)) AS sessions_percent
FROM
  HourlySessions
CROSS JOIN
  TotalSessions
ORDER BY
  hour