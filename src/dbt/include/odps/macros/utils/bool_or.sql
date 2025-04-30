
-- The boolean function is an aggregate function.
-- When there is true in the bool type group, it returns true, otherwise it returns false.

{% macro odps__bool_or(expression) -%}
    max({{ expression }})
{%- endmacro %}
