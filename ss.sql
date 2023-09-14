/*
MAX関数が2箇所に使用されていますが、それぞれ異なる目的で使われています。

MAX(IF(KEY = 'page_location', value.string_value, NULL)) AS page_location
MAX(IF(KEY = 'ga_session_id', value.int_value, NULL))
以下、それぞれのMAX関数の使用目的を説明します。

1. page_locationの取得
この部分は、event_paramsの中からpage_locationの値を取得するためのものです。GA4のデータモデルでは、event_paramsは配列として格納されており、各イベントには多くのパラメータが存在します。page_locationというキーを持つパラメータのみの値（この場合、value.string_value）を取得するために、IF条件を使用しています。

しかし、なぜMAXを使用するかというと、GROUP BY節でuser_pseudo_idによってグルーピングしているため、各ユーザーについて複数のレコードが生成される可能性があります。そのため、MAX関数を使用して、複数のレコードの中から最大のpage_locationの値（実際には1つしか存在しないはず）を取得しています。

2. session_idの生成
同様に、この部分はevent_paramsの中からga_session_idの値を取得して、それをuser_pseudo_idと結合して一意のセッションIDを生成するためのものです。MAX関数は、同様の理由で使用されています。

要するに、MAX関数は、複数のレコードから1つの値を選択するためのものとして使われています。GA4のデータモデルの特性上、UNNESTとGROUP BYを組み合わせて使用する場合、このようなテクニックが必要となります。
*/

SELECT
  page_location,
  COUNT(DISTINCT session_id) AS sessions
FROM (
  SELECT
    MAX(
    IF
      (KEY = 'page_location', value.string_value, NULL)) AS page_location,
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
    AND (KEY = 'page_location'
      OR KEY = 'ga_session_id')
  GROUP BY
    user_pseudo_id )
GROUP BY
  page_location
ORDER BY
  sessions DESC
LIMIT
  10;