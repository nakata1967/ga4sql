/*
�N�G�����̒��A���̌v�Z���@

�N�G�����ł�bounce_rate�̌v�Z���͈ȉ��̂悤�ɂȂ��Ă��܂��F

ROUND(100 * (1 - SAFE_DIVIDE(SUM(CAST(engaged_sessions AS INT)), COUNT(DISTINCT session_id))), 1)

���̎����X�e�b�v�o�C�X�e�b�v�ŕ������Ă݂܂��傤�B

�X�e�b�v1: engaged_sessions

���̕����ł́A�Z�b�V�������uengaged�v�ł��������ǂ����������l�����v���Ă��܂��B

�X�e�b�v2: �Z�b�V�������̃J�E���g
���ɁACOUNT(DISTINCT session_id)�őS�Z�b�V�����̐����J�E���g���Ă��܂��B

�X�e�b�v3: ���A���̌v�Z
���ɁA1 - SAFE_DIVIDE(���vengaged_sessions, �S�Z�b�V������)�Ƃ������ŁA���A�����Z�b�V�����̊������v�Z���Ă��܂��B

��̓I�ɂ́A�S�Z�b�V����������engaged�������Z�b�V�����̐��������A�����S�Z�b�V�������Ŋ��邱�ƂŁA���A�����v�Z���Ă��܂��B

�X�e�b�v4: �p�[�Z���e�[�W�\��
�Ō�ɁA���ʂ�100�{���ăp�[�Z���e�[�W�Ƃ��ĕ\�����Ă��܂��B�����āAROUND( ..., 1)���g�p���ď����_�ȉ���1���Ɋۂ߂Ă��܂��B
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