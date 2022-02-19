{{config(
    materialized='load_table',
    load_columns=[
      {'name':'id',   'data_type':'integer'},
      {'name':'name', 'data_type':'varchar'},
    ],
    from='s3://'~env_var('S3_BUCKET_NAME')~'/'~env_var('S3_KEY_PREFIX')~'test_load_table.csv',
    iam_role=env_var('IAM_ROLE_ARN'),
    copy_option="REGION 'ap-northeast-1' FORMAT csv IGNOREHEADER 1",
)}}

select *
from {{materialization_load_with.load_temporary_table()}}
