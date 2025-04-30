{% macro odps__validate_sql(sql) -%}
    explain {{ sql }}
{% endmacro %}
