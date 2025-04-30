-- 7 is the length of 'array()', I don't know how to judge if array is empty other than this method
{% macro odps__array_concat(array_1, array_2) -%}
    concat({{ array_1 }}, {{ array_2 }})
{%- endmacro %}
