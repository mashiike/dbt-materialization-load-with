{#-
  -- Basically, it is implemented by referring to the dbt-core macro 'is_incremental' process. (dbt version 1.0.2)
  -- The original macro 'is_incremental' code is shown below.
  -- https://github.com/dbt-labs/dbt-core/blob/v1.0.2/core/dbt/include/global_project/macros/materializations/models/incremental/is_incremental.sql
  --
  -- The code license for the original dbt-core macro 'is_incremental' is as follows
  -- https://github.com/dbt-labs/dbt-core/blob/v1.0.2/License.md
-#}
{% macro is_load_incremental() %}
    {#-- do not run introspective queries in parsing #}
    {% if not execute %}
        {{ return(False) }}
    {% else %}
        {% set relation = adapter.get_relation(this.database, this.schema, this.table) %}
        {{ return(relation is not none
                  and relation.type == 'table'
                  and model.config.materialized == 'load_incremental'
                  and not should_full_refresh()) }}
    {% endif %}
{% endmacro %}

{% macro is_incremental() %}
    {{ return(dbt.is_incremental() or materialization_load_with.is_load_incremental()) }}
{% endmacro %}
