{% macro odps__drop_table(relation) %}
    {% call statement(name="main") %}
    drop table if exists {{ relation }}
    {% endcall %}
{% endmacro %}
