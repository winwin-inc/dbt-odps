{% macro odps__array_append(array, new_element) -%}
    concat({{ array }}, array({{ new_element }}))
{%- endmacro %}
