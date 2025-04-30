{% materialization raw,  adapter='odps' -%}
  {{ adapter.run_raw_sql(sql, config) }}
  {% call statement("main") %}
  {% endcall %}
  {{ return({'relations': []}) }}
{%- endmaterialization %}
