WITH
  Sessions AS (
  SELECT
    user_pseudo_id,
    event_params.key AS session_key,
    event_params.value.int_value AS ga_session_id,
    MIN(event_timestamp) AS session_start_timestamp,
    MAX(event_timestamp) AS session_end_timestamp
  FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
    UNNEST(event_params) AS event_params
  WHERE
    _TABLE_SUFFIX BETWEEN '20201101'
    AND '20201110'
    AND event_params.key = 'ga_session_id'
  GROUP BY
    user_pseudo_id,
    session_key,
    ga_session_id ),
  Calculations AS (
  SELECT
    FORMAT_DATE('%Y/%m/%d', DATE(TIMESTAMP_MICROS(session_start_timestamp))) AS session_date,
    AVG((session_end_timestamp - session_start_timestamp) / 1000000) AS avg_session_duration_seconds
  FROM
    Sessions
  GROUP BY
    session_date )
SELECT
  session_date,
  CONCAT( CAST(FLOOR(avg_session_duration_seconds / 3600) AS STRING), ":", LPAD(CAST(FLOOR(SAFE_DIVIDE(avg_session_duration_seconds, 60) - FLOOR(avg_session_duration_seconds / 3600) * 60) AS STRING), 2, '0'), ":", LPAD(CAST(FLOOR(avg_session_duration_seconds - FLOOR(avg_session_duration_seconds / 60) * 60) AS STRING), 2, '0') ) AS avg_session_duration
FROM
  Calculations
ORDER BY
  session_date;