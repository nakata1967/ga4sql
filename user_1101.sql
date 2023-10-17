WITH
  SessionData AS (
  SELECT
    user_pseudo_id,
    event_param.value.int_value AS session_number,
    TIMESTAMP_MICROS(event_timestamp) AS session_time
  FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  CROSS JOIN
    UNNEST(event_params) AS event_param
  WHERE
    _TABLE_SUFFIX = '20201101'
    AND event_param.key = 'ga_session_number' ),
  SessionCounts AS (
  SELECT
    user_pseudo_id,
    COUNT(DISTINCT session_number) AS total_sessions
  FROM
    SessionData
  GROUP BY
    user_pseudo_id ),
  EarliestSessions AS (
  SELECT
    user_pseudo_id,
    MIN(session_number) AS earliest_session_number
  FROM
    SessionData
  WHERE
    user_pseudo_id IN (
    SELECT
      user_pseudo_id
    FROM
      SessionCounts
    WHERE
      total_sessions >= 2)
  GROUP BY
    user_pseudo_id )
SELECT
  s.user_pseudo_id,
  MAX(s.session_number) AS max_ga_session_number,
  CASE
    WHEN sc.total_sessions >= 2 THEN TRUE
  ELSE
  FALSE
END
  AS visited_more_than_once,
  COALESCE(es.earliest_session_number, NULL) AS earliest_session_number_for_multiple_visits
FROM
  SessionData s
LEFT JOIN
  SessionCounts sc
ON
  s.user_pseudo_id = sc.user_pseudo_id
LEFT JOIN
  EarliestSessions es
ON
  s.user_pseudo_id = es.user_pseudo_id
GROUP BY
  s.user_pseudo_id,
  sc.total_sessions,
  es.earliest_session_number
HAVING
  visited_more_than_once = TRUE
ORDER BY
  max_ga_session_number DESC;