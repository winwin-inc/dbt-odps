-- https://docs.getdbt.com/reference/dbt-jinja-functions/cross-database-macros#cast_bool_to_text
-- need to consider 'NULL'
{% macro odps__cast_bool_to_text(field) -%}
    tolower(cast({{ field }} as string))
{%- endmacro %}
