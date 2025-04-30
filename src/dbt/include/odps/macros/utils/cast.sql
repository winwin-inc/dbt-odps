-- https://help.aliyun.com/zh/odps/user-guide/cast
{% macro odps__cast(field, type) %}
    cast({{field}} as {{type}})
{% endmacro %}
