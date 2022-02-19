# dbt-materialization-load-with

A [dbt package](https://docs.getdbt.com/docs/building-a-dbt-project/package-management) for materialization with data loading
**Note: This is a PoC-like DBT package for Redshift.**

This DBT package mainly provides materialization for Redshift.
There are two materializations in the package, `load_table` and `load_incremental`.
These are based on the concept of loading data into a temporary table using [Redshift's COPY](https://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html) command, and then executing the table or incremental materialization based on the loaded temporary table.

## Installation

Add to your packages.yml
```yaml
packages:
  - git: "https://github.com/mashiike/dbt-materialization-load-with"
    revision: v0.0.0
```

It is also useful to prepare the following macro.

```
{% macro is_incremental() %}
  {{ return(materialization_load_with.is_incremental()) }}
{% endmacro %}

{% macro load_temporary_table() %}
  {{ return(materialization_load_with.load_temporary_table()) }}
{% endmacro %}
```

## QuickStart 

Assuming you have a CSV like the following, when you load it, it will look like this

csv:
```csv
id,name,age
1,hoge,18
2,fuga,28
3,piyo,38
```

simple_load_table.sql:
```sql
{{config(
    materialized='load_table',
    load_columns=[
      {'name':'id',   'data_type':'integer'},
      {'name':'name', 'data_type':'varchar'},
      {'name':'age', 'data_type':'integer'},
    ],
    from='s3://example-com/path/to/csv',
    iam_role=env_var('IAM_ROLE_ARN'),
    copy_option="REGION '"~env_var('AWS_DEFAULT_REGION','us-east-1')~"' FORMAT csv IGNOREHEADER 1",
)}}

select *
from {{load_temporary_table()}}
```

If you want to update it with an addendum, it will look like this
simple_load_incremental.sql:
```sql
{{config(
    materialized='load_table',
    load_columns=[
      {'name':'id',   'data_type':'integer'},
      {'name':'name', 'data_type':'varchar'},
      {'name':'age', 'data_type':'integer'},
    ],
    from='s3://example-com/path/to/csv',
    iam_role=env_var('IAM_ROLE_ARN'),
    copy_option="REGION '"~env_var('AWS_DEFAULT_REGION','us-east-1')~"' FORMAT csv IGNOREHEADER 1",
)}}

select *
from {{load_temporary_table()}} as l
{%- if is_incremental() %}
where not exists (
  select 1 
  from {{ this }} as t
  where l.id = t.id
)
{%- endif %}
```

Note: In the case of `load_incremental`, it is difficult to switch the config dynamically; it is very useful if the from is fixed or can be specified by an environment variable.

## LICENSE

MIT 

However, some of the code has been modified from https://github.com/dbt-labs/dbt-core, the original license of which is [here](https://github.com/dbt-labs/dbt-core/blob/v1.0.2/License.md)
