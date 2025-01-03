{#
# Copyright 2022 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#}


{% macro odps__reset_csv_table(model, full_refresh, old_relation, agate_table) %}
    {% if old_relation %}
        {{ adapter.drop_relation(old_relation) }}
    {% endif %}
    {% set sql = create_csv_table(model, agate_table) %}
    {{ return(sql) }}
{% endmacro %}

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
                {%- if row[column] is none -%}
                    null
                {%- elif data_types[column] == 'string' -%}
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

{% macro odps__create_csv_table(model, agate_table) %}
  {%- set column_override = model['config'].get('column_types', {}) -%}
  {%- set quote_seed_column = model['config'].get('quote_columns', None) -%}

  {% set sql %}
    create table {{ this.render() }} (
        {%- for col_name in agate_table.column_names -%}
            {%- set inferred_type = adapter.convert_type(agate_table, loop.index0) -%}
            {%- set type = column_override.get(col_name, inferred_type) -%}
            {%- set column_name = (col_name | string) -%}
            {{ adapter.quote_seed_column(column_name, quote_seed_column) }} {{ type }} {%- if not loop.last -%}, {%- endif -%}
        {%- endfor -%}
    )
    {{ file_format_clause() }}
    {{ partition_cols(label="partitioned by") }}
    {{ clustered_cols(label="clustered by") }}
    {{ location_clause() }}
    {{ comment_clause() }}
  {% endset %}

  {% call statement('_') -%}
    {{ sql }}
  {%- endcall %}

  {{ return(sql) }}
{% endmacro %}


{% materialization seed, adapter='odps' %}

  {%- set identifier = model['alias'] -%}
  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set target_relation = api.Relation.create(database=database, schema=schema, identifier=identifier, type='table') -%}
  {%- set agate_table = load_agate_table() -%}
  {%- do store_result('agate_table', response='OK', agate_table=agate_table) -%}

  {{ run_hooks(pre_hooks) }}

  -- build model
  {% set create_table_sql = reset_csv_table(model, full_refresh_mode, old_relation, agate_table) %}
  {% set status = 'CREATE' %}
  {% set num_rows = (agate_table.rows | length) %}
  {% set sql = load_csv_rows(model, agate_table) %}

  {% call noop_statement('main', status ~ ' ' ~ num_rows) %}
    {{ create_table_sql }};
    -- dbt seed --
    {{ sql }}
  {% endcall %}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
