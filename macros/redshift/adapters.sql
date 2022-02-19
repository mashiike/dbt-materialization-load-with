{%- macro redshift__create_load_temporary_table(relation, config) -%}
  {% set load_columns = config.get('load_columns', validator=validation.any[list]) %}
  {%- set _dist = config.get('dist') -%}
  {%- set _sort_type = config.get(
          'sort_type',
          validator=validation.any['compound', 'interleaved']) -%}
  {%- set _sort = config.get(
          'sort',
          validator=validation.any[list, basestring]) -%}

  {%- set sql_header = config.get('sql_header', none) -%}
  {{ sql_header if sql_header is not none }}

  create temporary table {{ relation.include(database=False, schema=False) }} (
    {%- for column in load_columns %}
      {%- if column is not mapping %}
        {{ exceptions.raise_compiler_error("Invalid `load_columns` elements Got: " ~ column) }}
      {%- endif %}
      {{ adapter.quote(column.name) }} {{ column.data_type }} {% if not loop.last %},{%- endif %}
    {%- endfor %}
  )
  {{ dist(_dist) }}
  {{ sort(_sort_type, _sort) }};
{%- endmacro %}

{%- macro redshift__copy_into_load_temporary_table(relation, config) -%}
  {%- do config.require('from') %}
  {%- set copy_from = config.get('from', validator=validation.any[basestring]) %}
  copy {{ relation.include(database=False, schema=False) }}
  from '{{ copy_from }}'
  {%- set aws_iam_role = config.get('iam_role', validator=validation.any[basestring],default=none) %}
  {%- if aws_iam_role is not none %}
  iam_role '{{ aws_iam_role }}'
  {%- else %}
  {%- set aws_credentials = config.get('credentials', validator=validation.any[basestring], default=none) %}
    {%- if  aws_credentials is not none %}
  credentials '{{ aws_credentials }}'
    {%- else %}
      {{ exceptions.raise_compiler_error("Either 'iam_role' or 'credentials' needs to be specified") }}
    {%- endif %}
  {%- endif %}
  {%- set copy_option = config.get('copy_option', validator=validation.any[basestring]) %}
  {{ copy_option }};
{%- endmacro %}
