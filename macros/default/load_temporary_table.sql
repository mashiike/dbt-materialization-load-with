{%- macro load_temporary_table() %}
  {%- set temp_identifier = '__'~this.schema~'__'~this.identifier ~ '__dbt_load_tmp' %}
  {%- set temp_relation = api.Relation.create(database=None, schema=None, identifier=temp_identifier, type='table') %}
  {{ return(temp_relation) }}
{%- endmacro %}
