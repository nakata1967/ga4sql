/*
下記のクエリでのMAX関数の使用目的を説明します。

MAX(IF(KEY = 'source', value.string_value, NULL))
MAX(IF(KEY = 'medium', value.string_value, NULL))
MAX(IF(KEY = 'ga_session_id', value.int_value, NULL))

1. sourceの取得:

MAX(IF(KEY = 'source', value.string_value, NULL))

この部分は、event_paramsの中からsourceの値を取得するためのものです。event_paramsは配列として格納され、各イベントには多くのパラメータが存在します。特定のキー（この場合source）を持つパラメータの値（value.string_value）を取得するために、IF条件を使用しています。

MAX関数の使用目的は、GROUP BY節でuser_pseudo_idによってグルーピングすると、各ユーザーに対して複数のレコードが生成される可能性があるためです。このMAX関数は、複数のレコードの中から最大のsourceの値（実際には1つしか存在しないはず）を取得するために使用されています。

2. mediumの取得:

MAX(IF(KEY = 'medium', value.string_value, NULL))

この部分は同様のロジックに従って、event_paramsからmediumの値を取得するためのものです。

3. session_idの生成:

MAX(IF(KEY = 'ga_session_id', value.int_value, NULL))

この部分は、event_paramsの中からga_session_idの値を取得し、それをuser_pseudo_idと結合して一意のセッションIDを生成するためのものです。同様の理由で、複数のレコードの中から最大のga_session_idの値を取得しています。

要約すると、MAX関数はGROUP BYによって生成される可能性のある複数のレコードから1つの値を取得するために使用されています。これはGA4のデータモデルの特性と、この特定のクエリのロジックの組み合わせに起因しています。
*/

SELECT
  traffic_source,
  COUNT(DISTINCT session_id) AS sessions
FROM (
  SELECT
    CONCAT(MAX(
      IF
        (KEY = 'source', value.string_value, NULL)), '/', MAX(
      IF
        (KEY = 'medium', value.string_value, NULL))) AS traffic_source,
    CONCAT( user_pseudo_id, MAX(
      IF
        (KEY = 'ga_session_id', value.int_value, NULL)) ) AS session_id
  FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
    UNNEST(event_params) AS event_param
  WHERE
    _TABLE_SUFFIX BETWEEN '20201101'
    AND '20221130'
    AND event_name = 'page_view'
    AND (KEY = 'source'
      OR KEY = 'medium'
      OR KEY = 'ga_session_id')
  GROUP BY
    user_pseudo_id )
WHERE
  traffic_source IS NOT NULL
GROUP BY
  traffic_source
ORDER BY
  sessions DESC
LIMIT
  10;