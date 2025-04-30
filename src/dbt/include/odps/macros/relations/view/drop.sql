{% macro odps__drop_view(relation) %}
    {% call statement(name="main") %}
    drop view if exists {{ relation }}
    {% endcall %}
{% endmacro %}
