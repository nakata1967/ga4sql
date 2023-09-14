/*
���L�̃N�G���ł�MAX�֐��̎g�p�ړI��������܂��B

MAX(IF(KEY = 'source', value.string_value, NULL))
MAX(IF(KEY = 'medium', value.string_value, NULL))
MAX(IF(KEY = 'ga_session_id', value.int_value, NULL))

1. source�̎擾:

MAX(IF(KEY = 'source', value.string_value, NULL))

���̕����́Aevent_params�̒�����source�̒l���擾���邽�߂̂��̂ł��Bevent_params�͔z��Ƃ��Ċi�[����A�e�C�x���g�ɂ͑����̃p�����[�^�����݂��܂��B����̃L�[�i���̏ꍇsource�j�����p�����[�^�̒l�ivalue.string_value�j���擾���邽�߂ɁAIF�������g�p���Ă��܂��B

MAX�֐��̎g�p�ړI�́AGROUP BY�߂�user_pseudo_id�ɂ���ăO���[�s���O����ƁA�e���[�U�[�ɑ΂��ĕ����̃��R�[�h�����������\�������邽�߂ł��B����MAX�֐��́A�����̃��R�[�h�̒�����ő��source�̒l�i���ۂɂ�1�������݂��Ȃ��͂��j���擾���邽�߂Ɏg�p����Ă��܂��B

2. medium�̎擾:

MAX(IF(KEY = 'medium', value.string_value, NULL))

���̕����͓��l�̃��W�b�N�ɏ]���āAevent_params����medium�̒l���擾���邽�߂̂��̂ł��B

3. session_id�̐���:

MAX(IF(KEY = 'ga_session_id', value.int_value, NULL))

���̕����́Aevent_params�̒�����ga_session_id�̒l���擾���A�����user_pseudo_id�ƌ������Ĉ�ӂ̃Z�b�V����ID�𐶐����邽�߂̂��̂ł��B���l�̗��R�ŁA�����̃��R�[�h�̒�����ő��ga_session_id�̒l���擾���Ă��܂��B

�v�񂷂�ƁAMAX�֐���GROUP BY�ɂ���Đ��������\���̂��镡���̃��R�[�h����1�̒l���擾���邽�߂Ɏg�p����Ă��܂��B�����GA4�̃f�[�^���f���̓����ƁA���̓���̃N�G���̃��W�b�N�̑g�ݍ��킹�ɋN�����Ă��܂��B
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