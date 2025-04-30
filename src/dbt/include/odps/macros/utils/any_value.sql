{# https://help.aliyun.com/zh/odps/user-guide/any-value #}
{% macro odps__any_value(expression) -%}

    any_value({{ expression }})

{%- endmacro %}
