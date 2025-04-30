{% macro odps__listagg(measure, delimiter_text, order_by_clause, limit_num) -%}
    {% if limit_num != None -%}
       {{ exceptions.raise_compiler_error("macro listagg not support set limit_num") }}
    {% endif -%}
    wm_concat({{delimiter_text}},{{measure}})
    {% if order_by_clause != None -%}
       within group ({{order_by_clause}})
    {% endif -%}
{%- endmacro %}
