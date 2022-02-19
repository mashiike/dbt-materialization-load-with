{#-
  -- Basically, it is implemented by referring to the dbt-core materialization 'table' process. (dbt version 1.0.2)
  -- The original materialization 'table' code is shown below.
  -- https://github.com/dbt-labs/dbt-core/blob/v1.0.2/core/dbt/include/global_project/macros/materializations/models/table/table.sql
  --
  -- The code license for the original dbt-core materialization 'table' is as follows
  -- https://github.com/dbt-labs/dbt-core/blob/v1.0.2/License.md
-#}
{% materialization load_table, default -%}
  -- additional required config parameteor
  {%- do config.require('load_columns') %}

  {% set target_relation = this.incorporate(type='table') %}
  {% set existing_relation = load_relation(this) %}
  {%- set load_temp_relation = materialization_load_with.load_temporary_table() %}

  {%- set tmp_identifier = model['name'] + '__dbt_tmp' -%}
  {%- set intermediate_relation = target_relation.incorporate(path={"identifier": tmp_identifier}) %}

  {%- set backup_identifier = model['name'] + '__dbt_backup' -%}
  {%- set backup_relation_type = 'table' if existing_relation is none else existing_relation.type -%}
  {%- set backup_relation = this.incorporate(path={"identifier":backup_identifier},type=backup_relation_type) -%}

  {%- set preexisting_intermediate_relation = load_relation(intermediate_relation) -%}
  {%- set preexisting_backup_relation = load_relation(backup_relation) -%}

  -- drop the temp relations if they exist already in the database
  {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
  {{ drop_relation_if_exists(preexisting_backup_relation) }}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

   -- build model
  {% call statement("main") %}
      -- data load into temporary table
      {{ materialization_load_with.get_create_and_copy_into_load_temporary_table_sql(load_temp_relation, config) }}

      -- create intermediate table
      {{ get_create_table_as_sql(False, intermediate_relation, sql) }}
  {% endcall %}

   -- cleanup
  {% if existing_relation is not none %}
      -- now existing table rename to backup
      {{ adapter.rename_relation(existing_relation, backup_relation) }}
  {% endif %}

  -- intermediate table rename to target table
  {{ adapter.rename_relation(intermediate_relation, target_relation) }}

  {% do create_indexes(target_relation) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {% do adapter.commit() %}

  -- finally, drop the existing/backup relation after the commit
  {{ drop_relation_if_exists(backup_relation) }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}
{%- endmaterialization %}
