{% macro is_incremental() %}
  {{ return(materialization_load_with.is_incremental()) }}
{% endmacro %}
