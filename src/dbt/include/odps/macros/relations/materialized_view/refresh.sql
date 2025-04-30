{% macro odps__refresh_materialized_view(relation) %}
    ALTER MATERIALIZED VIEW {{ relation.render() }} REBUILD;
{% endmacro %}
