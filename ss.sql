/*
MAX�֐���2�ӏ��Ɏg�p����Ă��܂����A���ꂼ��قȂ�ړI�Ŏg���Ă��܂��B

MAX(IF(KEY = 'page_location', value.string_value, NULL)) AS page_location
MAX(IF(KEY = 'ga_session_id', value.int_value, NULL))
�ȉ��A���ꂼ���MAX�֐��̎g�p�ړI��������܂��B

1. page_location�̎擾
���̕����́Aevent_params�̒�����page_location�̒l���擾���邽�߂̂��̂ł��BGA4�̃f�[�^���f���ł́Aevent_params�͔z��Ƃ��Ċi�[����Ă���A�e�C�x���g�ɂ͑����̃p�����[�^�����݂��܂��Bpage_location�Ƃ����L�[�����p�����[�^�݂̂̒l�i���̏ꍇ�Avalue.string_value�j���擾���邽�߂ɁAIF�������g�p���Ă��܂��B

�������A�Ȃ�MAX���g�p���邩�Ƃ����ƁAGROUP BY�߂�user_pseudo_id�ɂ���ăO���[�s���O���Ă��邽�߁A�e���[�U�[�ɂ��ĕ����̃��R�[�h�����������\��������܂��B���̂��߁AMAX�֐����g�p���āA�����̃��R�[�h�̒�����ő��page_location�̒l�i���ۂɂ�1�������݂��Ȃ��͂��j���擾���Ă��܂��B

2. session_id�̐���
���l�ɁA���̕�����event_params�̒�����ga_session_id�̒l���擾���āA�����user_pseudo_id�ƌ������Ĉ�ӂ̃Z�b�V����ID�𐶐����邽�߂̂��̂ł��BMAX�֐��́A���l�̗��R�Ŏg�p����Ă��܂��B

�v����ɁAMAX�֐��́A�����̃��R�[�h����1�̒l��I�����邽�߂̂��̂Ƃ��Ďg���Ă��܂��BGA4�̃f�[�^���f���̓�����AUNNEST��GROUP BY��g�ݍ��킹�Ďg�p����ꍇ�A���̂悤�ȃe�N�j�b�N���K�v�ƂȂ�܂��B
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