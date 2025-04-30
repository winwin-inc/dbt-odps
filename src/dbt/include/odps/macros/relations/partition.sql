{% macro partition_by(partition_config) -%}
    {%- if partition_config is none -%}
      {% do return('') %}
    {%- elif partition_config.auto_partition() -%}
      {%- if partition_config.generate_column_name is none -%}
        auto partitioned by (trunc_time(`{{ partition_config.fields[0] }}`, "{{ partition_config.granularity }}"))
      {%- else -%}
        auto partitioned by (trunc_time(`{{ partition_config.fields[0] }}`, "{{ partition_config.granularity }}") as `{{ partition_config.generate_column_name }}`)
      {%- endif -%}
    {%- else -%}
        partitioned by ({{ partition_config.render() }})
    {%- endif -%}
{%- endmacro -%}
