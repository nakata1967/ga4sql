/*
1. 基本の構造:

- 外側のクエリは、内側のサブクエリの結果を元にして集計を行っています。サブクエリが各ユーザのセッション情報と該当セッションのmediumを取得し、外側のクエリがこれを集計して上位10のmediumとそのセッション数を取得します。

2. サブクエリの説明:

- GA4のテーブルから、イベント名がpurchaseまたはpage_viewであるデータを対象期間内で抽出しています。
- UNNEST(event_params) AS event_paramを使って、event_params配列を展開しています。
- WHERE句では、対象となるmediumとga_session_idのキーのデータのみをフィルタリングしています。
- SELECT句で、各ユーザのセッションに関連するmediumとセッションIDを取得しています。
- HAVING句を用いて、そのセッションでpurchaseイベントが発火したユーザのみをフィルタリングしています。

3. 外側のクエリの説明:

- サブクエリの結果を元に、mediumごとにセッション数を集計しています。
- medium IS NOT NULLを使用して、mediumがNULLでないデータのみを対象としています。
- ORDER BY sessions DESCでセッション数の多い順にソートしています。
- LIMIT 10で上位10の結果のみを取得しています。
*/
SELECT
  medium,
  COUNT(DISTINCT session_id) AS sessions
FROM (
  SELECT
    MAX(IF(KEY = 'medium', value.string_value, NULL)) AS medium,
    CONCAT(user_pseudo_id, MAX(
      IF(KEY = 'ga_session_id', value.int_value, NULL))) AS session_id
  FROM
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
    UNNEST(event_params) AS event_param
  WHERE
    _TABLE_SUFFIX BETWEEN '20201101'
    AND '20201110'
    AND event_name IN ('purchase', 'page_view')
    AND (KEY = 'medium' OR KEY = 'ga_session_id')
  GROUP BY
    user_pseudo_id
  HAVING 
    MAX(IF(event_name = 'purchase', 1, NULL)) IS NOT NULL) -- purchaseイベントが発火したセッションのみ
WHERE
  medium IS NOT NULL
GROUP BY
  medium
ORDER BY
  sessions DESC
LIMIT
  10;
