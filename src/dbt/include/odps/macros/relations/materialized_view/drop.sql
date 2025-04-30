{% macro odps__drop_materialized_view(relation) -%}
    {% call statement(name="main") %}
    {{- log("replace materialized view will drop it first and then recreate.") -}}
    drop materialized view if exists {{ relation.render() }}
    {% endcall %}
{%- endmacro %}
