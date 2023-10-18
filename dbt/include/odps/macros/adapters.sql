{% macro lifecycle_clause(temporary) %}
  {{ return(adapter.dispatch('lifecycle_clause', 'dbt')(temporary)) }}
{%- endmacro -%}


{% macro odps__lifecycle_clause(temporary) %}
  {%- set lifecycle = config.get('lifecycle') -%}
  {%- if lifecycle is not none -%}
    lifecycle {{ lifecycle }}
  {%- elif temporary -%}
    lifecycle 1
  {%- endif %}
{%- endmacro -%}


{% macro properties_clause() %}
  {{ return(adapter.dispatch('properties_clause', 'dbt')()) }}
{%- endmacro -%}


{% macro odps__properties_clause() %}
  {%- set properties = config.get('properties', none) -%}
  {%- if properties is not none -%}
    tblproperties(
    {%- for k, v in properties.items() -%}'{{ k }}'='{{ v }}'
    {%- if not loop.last %},{% endif -%}
    {%- endfor -%}
    )
  {%- endif %}
{%- endmacro -%}


{% macro partitioned_by_clause() %}
  {{ return(adapter.dispatch('partitioned_by_clause', 'dbt')()) }}
{%- endmacro -%}


{% macro odps__partitioned_by_clause() %}
  {%- set partitioned_by = config.get('partitioned_by', none) -%}
  {%- if partitioned_by is not none -%}
    partitioned by (
    {%- for item in partitioned_by -%}
      {{ item['col_name'] }} {{ item['data_type'] or 'string' }}{%- if item['comment'] %} comment '{{ item['comment'] }}'{%- endif -%}
      {%- if not loop.last %}, {% endif -%}
    {%- endfor -%}
    )
  {%- endif %}
{%- endmacro -%}


{%- macro odps__current_timestamp() -%}
  current_timestamp()
{%- endmacro -%}


{% macro odps__drop_relation(relation) -%}
  {% call statement('drop_relation', auto_begin=False) -%}
    drop {{ relation.type }} if exists {{ relation }}
  {%- endcall %}
{% endmacro %}

{# tested: using cast to convert types #}
{% macro odps__load_csv_rows(model, agate_table) %}
  {% set batch_size = get_batch_size() %}
  {% set cols_sql = get_seed_column_quoted_csv(model, agate_table.column_names) %}
  {% set bindings = [] %}
  {% set statements = [] %}

  {# get odps types #}
  {% set column_override = model['config'].get('column_types', {}) %}
  {% set data_types = {} %}
  {% for col_name in agate_table.column_names %}
    {% set inferred_type = adapter.convert_type(agate_table, loop.index0) %}
    {% set data_type = column_override.get(col_name, inferred_type) %}
    {% do data_types.update({col_name: data_type}) %}
  {% endfor %}

  {% for chunk in agate_table.rows | batch(batch_size) %}
      {% set sql %}
          insert into {{ this.render() }} ({{ cols_sql }}) values
          {% for row in chunk -%}
              ({%- for column in agate_table.column_names -%}
                  {%- if data_types[column] == 'string' -%}
                    '{{ row[column] }}'
                  {%- else -%}
                    cast('{{ row[column] }}' as {{ data_types[column] }})
                  {%- endif -%}
                  {%- if not loop.last%},{%- endif %}
              {%- endfor -%})
              {%- if not loop.last%},{%- endif %}
          {%- endfor %}
      {% endset %}

      {% do adapter.add_query(sql, abridge_sql_log=True) %}

      {% if loop.index0 == 0 %}
          {% do statements.append(sql) %}
      {% endif %}
  {% endfor %}

  {{ return(statements[0]) }}
{% endmacro %}

{# tested #}
{% macro odps__create_view_as(relation, sql) -%}
  create or replace view {{ relation }}
    {% set contract_config = config.get('contract') %}
    {% if contract_config.enforced %}
      {{ get_assert_columns_equivalent(sql) }}
    {%- endif %}
  as {{ sql }};
{%- endmacro %}

{# tested #}
{% macro odps__create_table_as(temporary, relation, sql) -%}
  {%- if temporary -%}
    {% call statement('drop_before_create') -%}
      {%- if not relation.type -%}
        {% do exceptions.raise_database_error("Cannot drop a relation with a blank type: " ~ relation.identifier) %}
      {%- elif relation.type in ('table') -%}
          drop table if exists {{ relation }}
      {%- elif relation.type == 'view' -%}
          drop view if exists {{ relation }}
      {%- else -%}
        {% do exceptions.raise_database_error("Unknown type '" ~ relation.type ~ "' for relation: " ~ relation.identifier) %}
      {%- endif -%}
    {%- endcall -%}
  {%- endif -%}

  create table if not exists {{ relation }}
    {% set contract_config = config.get('contract') %}
    {% if contract_config.enforced %}
      {{ get_assert_columns_equivalent(sql) }}
      {{ get_columns_spec_ddl() }}
      {%- set sql = get_select_subquery(sql) %}
    {%- endif %}
    {{ partitioned_by_clause() }}
    {{ properties_clause() }}
    {{ lifecycle_clause(temporary) }}
    {# {{ clustered_by_clause() }} --not supported yet #}
    {# {{ partitioned_by_clause() }} --not supported yet #}
    {# {{ properties_clause() }} --not supported yet #}
  as {{ sql }}
{%- endmacro %}


{# tested #}
{% macro odps__rename_relation(from_relation, to_relation) -%}
  {% set target_name = adapter.quote_as_configured(to_relation.identifier, 'identifier') %}

  {% call statement('rename_relation') -%}
    {% if not from_relation.type %}
      {% do exceptions.raise_database_error("Cannot rename a relation with a blank type: " ~ from_relation.identifier) %}
    {% elif from_relation.type in ('table') %}
        alter table {{ from_relation }} rename to {{ target_name }}
    {% elif from_relation.type == 'view' %}
        alter view {{ from_relation }} rename to {{ target_name }}
    {% else %}
      {% do exceptions.raise_database_error("Unknown type '" ~ from_relation.type ~ "' for relation: " ~ from_relation.identifier) %}
    {% endif %}
  {%- endcall %}
{% endmacro %}

{# tested #}
{% macro show_create_table(relation) %}
  {% call statement('show_create_table', fetch_result=True) -%}
    show create table {{ relation }}
  {%- endcall %}

  {% set result = load_result('show_create_table') %}
  {% do return(result.table[0][0]) %}
{% endmacro %}
