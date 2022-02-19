{%- set s3_url_prefix = 's3://'~env_var('S3_BUCKET_NAME')~'/'~env_var('S3_KEY_PREFIX') %}
{%- if is_incremental() %}
  {%- set s3_url = s3_url_prefix~'test_load_incremental.csv' %}
{%- else %}
  {%- set s3_url = s3_url_prefix~'test_load_table.csv' %}
{%- endif %}
{{config(
    materialized='load_incremental',
    load_columns=[
      {'name':'id',   'data_type':'integer'},
      {'name':'name', 'data_type':'varchar'},
    ],
    from=s3_url,
    iam_role=env_var('IAM_ROLE_ARN'),
    copy_option="REGION 'ap-northeast-1' FORMAT csv IGNOREHEADER 1",
)}}

select *
from {{materialization_load_with.load_temporary_table()}}
