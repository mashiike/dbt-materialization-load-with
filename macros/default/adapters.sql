{%- macro create_load_temporary_table(relation, config) -%}
  {{ adapter.dispatch('create_load_temporary_table', 'materialization_load_with')(relation, config) }}
{%- endmacro %}

{%- macro default__create_load_temporary_table(relation, config) -%}
  {% set load_columns = config.get('load_columns', validator=validation.any[list]) %}

  {%- set sql_header = config.get('sql_header', none) -%}
  {{ sql_header if sql_header is not none }}

  create temporary table {{ relation.include(database=False, schema=False) }} (
    {%- for column in load_columns %}
      {%- if column is not mapping %}
        {{ exceptions.raise_compiler_error("Invalid `load_columns` elements Got: " ~ column) }}
      {%- endif %}
      {{ adapter.quote(column.name) }} {{ column.data_type }} {% if not loop.last %},{%- endif %}
    {%- endfor %}
  );
{%- endmacro %}

{%- macro copy_into_load_temporary_table(relation, config) -%}
  {{ adapter.dispatch('copy_into_load_temporary_table', 'materialization_load_with')(relation, config) }}
{%- endmacro %}

{%- macro default__copy_into_load_temporary_table(relation, config) -%}
  {{ exceptions.raise_compiler_error("copy_into_load_temporary_table() is not implemented") }}
{%- endmacro %}

{%- macro get_create_and_copy_into_load_temporary_table_sql(relation, config) -%}
  {{ adapter.dispatch('get_create_and_copy_into_load_temporary_table_sql', 'materialization_load_with')(relation, config) }}
{%- endmacro %}

{%- macro default__get_create_and_copy_into_load_temporary_table_sql(relation, config) -%}
  {{ materialization_load_with.create_load_temporary_table(relation, config) }}
  {{ materialization_load_with.copy_into_load_temporary_table(relation, config) }}
{%- endmacro %}
