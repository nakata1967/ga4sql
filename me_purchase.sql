/*
1. ��{�̍\��:

- �O���̃N�G���́A�����̃T�u�N�G���̌��ʂ����ɂ��ďW�v���s���Ă��܂��B�T�u�N�G�����e���[�U�̃Z�b�V�������ƊY���Z�b�V������medium���擾���A�O���̃N�G����������W�v���ď��10��medium�Ƃ��̃Z�b�V���������擾���܂��B

2. �T�u�N�G���̐���:

- GA4�̃e�[�u������A�C�x���g����purchase�܂���page_view�ł���f�[�^��Ώۊ��ԓ��Œ��o���Ă��܂��B
- UNNEST(event_params) AS event_param���g���āAevent_params�z���W�J���Ă��܂��B
- WHERE��ł́A�ΏۂƂȂ�medium��ga_session_id�̃L�[�̃f�[�^�݂̂��t�B���^�����O���Ă��܂��B
- SELECT��ŁA�e���[�U�̃Z�b�V�����Ɋ֘A����medium�ƃZ�b�V����ID���擾���Ă��܂��B
- HAVING���p���āA���̃Z�b�V������purchase�C�x���g�����΂������[�U�݂̂��t�B���^�����O���Ă��܂��B

3. �O���̃N�G���̐���:

- �T�u�N�G���̌��ʂ����ɁAmedium���ƂɃZ�b�V���������W�v���Ă��܂��B
- medium IS NOT NULL���g�p���āAmedium��NULL�łȂ��f�[�^�݂̂�ΏۂƂ��Ă��܂��B
- ORDER BY sessions DESC�ŃZ�b�V�������̑������Ƀ\�[�g���Ă��܂��B
- LIMIT 10�ŏ��10�̌��ʂ݂̂��擾���Ă��܂��B
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
    MAX(IF(event_name = 'purchase', 1, NULL)) IS NOT NULL) -- purchase�C�x���g�����΂����Z�b�V�����̂�
WHERE
  medium IS NOT NULL
GROUP BY
  medium
ORDER BY
  sessions DESC
LIMIT
  10;
