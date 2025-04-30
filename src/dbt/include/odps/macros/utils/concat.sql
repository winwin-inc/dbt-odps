{% macro odps__concat(fields) -%}
    concat({{ fields|join(', ') }})
{%- endmacro %}
