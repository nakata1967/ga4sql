WITH
  Sequenced AS (
  SELECT
    user_pseudo_id,
    ARRAY_AGG(page_location
    ORDER BY
      event_timestamp) AS pages
  FROM (
    SELECT
      event_timestamp,
      user_pseudo_id,
      event_param.value.string_value AS page_location
    FROM
      `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
      UNNEST(event_params) AS event_param
    WHERE
      _TABLE_SUFFIX BETWEEN '20201101'
      AND '20201110'
      AND event_name = 'page_view'
      AND event_param.key = 'page_location' )
  GROUP BY
    user_pseudo_id ),
  DistinctSequenced AS (
  SELECT
    user_pseudo_id,
    ARRAY(
    SELECT
      DISTINCT page
    FROM (
      SELECT
        pages[
      OFFSET
        (n)] AS page,
      IF
        (n=0, NULL, pages[
        OFFSET
          (n-1)]) AS previous_page
      FROM
        UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(pages) - 1)) AS n
      WHERE
        pages[
      OFFSET
        (n)] !=
      IF
        (n=0, NULL, pages[
        OFFSET
          (n-1)]) ) ) AS filtered_pages
  FROM
    Sequenced
  WHERE
    ARRAY_LENGTH(pages) > 0 ) -- “‡‚µ‚½ƒ‰ƒ“ƒLƒ“ƒO‚Ì¶¬
SELECT
  filtered_pages[SAFE_OFFSET(0)] AS landing,
IF
  (ARRAY_LENGTH(filtered_pages) > 1, filtered_pages[SAFE_OFFSET(1)], NULL) AS next_page,
IF
  (ARRAY_LENGTH(filtered_pages) > 2, filtered_pages[SAFE_OFFSET(2)], NULL) AS next_next_page,
  COUNT(user_pseudo_id) AS sessions
FROM
  DistinctSequenced
WHERE
  ARRAY_LENGTH(filtered_pages) > 0
GROUP BY
  landing,
  next_page,
  next_next_page
ORDER BY
  sessions DESC
LIMIT
  100;