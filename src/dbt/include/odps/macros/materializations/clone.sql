{% macro odps__can_clone_table() %}
    {{ return(True) }}
{% endmacro %}


{% macro odps__create_or_replace_clone(this_relation, defer_relation) %}
    {% call statement('drop_table', auto_begin=False) -%}
        drop table if exists {{ this_relation.render() }};
    {% endcall -%}
    clone table {{ defer_relation.render() }} to {{ this_relation.render() }};
{% endmacro %}
