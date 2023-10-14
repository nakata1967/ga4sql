SELECT
    params.value.string_value AS page_location,
    COUNT(*) AS pv_count
FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
    UNNEST (event_params) AS params
WHERE
    _TABLE_SUFFIX BETWEEN '20201101' AND '20201110'
    AND params.key = 'page_location'
    AND event_name = 'page_view'
GROUP BY
    page_location
ORDER BY
    pv_count DESC
LIMIT
    10