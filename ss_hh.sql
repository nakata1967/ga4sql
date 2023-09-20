WITH
  TotalSessions AS ( -- �Z�b�V�����̑������W�v
  SELECT
    COUNT(DISTINCT event_bundle_sequence_id) AS total_sessions
  FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE
    _TABLE_SUFFIX BETWEEN '20201101'
    AND '20201110'
    AND event_name = 'session_start' ),
  HourlySessions AS ( -- �e���ԑт��Ƃ̃Z�b�V���������W�v
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
    hour ) -- �e���ԑт��Ƃ̃Z�b�V�����̊������v�Z
SELECT
  FORMAT('%02d', hour) AS hh,
  FORMAT("%.3f%%", ROUND((session_count * 100.0 / total_sessions), 3)) AS sessions_percent
FROM
  HourlySessions
CROSS JOIN
  TotalSessions
ORDER BY
  hour