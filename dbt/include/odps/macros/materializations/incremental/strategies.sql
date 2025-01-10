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

{% macro get_insert_overwrite_sql(source_relation, target_relation, sql) %}
     {%- set sql_header = config.get('sql_header', none) %}
    {{ sql_header if sql_header is not none }}

    {%- set source_columns = odps__get_columns_from_query(sql) -%}
    {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}

    {%- do log('source_columns: ' ~ source_columns|join(',') ) -%}
    {%- do log('dest_columns: ' ~ dest_columns|join(',') ) -%}

    {% do odps__assert_columns_equals(source_columns, dest_columns) %}
    insert overwrite table {{ target_relation }}
    {{ partition_cols(label="partition") }}
    {{ sql }}

{% endmacro %}


{% macro get_insert_into_sql(source_relation, target_relation, sql) %}
     {%- set sql_header = config.get('sql_header', none) %}
    {{ sql_header if sql_header is not none }}

    {%- set source_columns = odps__get_columns_from_query(sql) -%}
    {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}

    {%- do log('source_columns: ' ~ source_columns|join(',') ) -%}
    {%- do log('dest_columns: ' ~ dest_columns|join(',') ) -%}

    {% do odps__assert_columns_equals(source_columns, dest_columns) %}
    insert into table {{ target_relation }}
    {{ partition_cols(label="partition") }}
    {{ sql }}

{% endmacro %}

{% macro get_qualified_columnnames_csv(columns, qualifier='') %}
    {% set quoted = [] %}
    {% for col in columns -%}
        {% if qualifier != '' %}
          {%- do quoted.append(qualifier + '.' + col.name) -%}
        {% else %}
          {%- do quoted.append(col.name) -%}
        {% endif %}
    {%- endfor %}

    {%- set dest_cols_csv = quoted | join(', ') -%}
    {{ return(dest_cols_csv) }}

{% endmacro %}

{% macro odps__get_merge_sql(target, source, unique_key, dest_columns, predicates=none) %}
  {%- set sql_header = config.get('sql_header', none) %}
    {{ sql_header if sql_header is not none }}
    
  {%- set predicates = [] if predicates is none else [] + predicates -%}
  {%- set merge_update_columns = config.get('merge_update_columns') -%}
  {%- set merge_exclude_columns = config.get('merge_exclude_columns') -%}
  {%- set update_columns = get_merge_update_columns(merge_update_columns, merge_exclude_columns, dest_columns) -%}

  {% if unique_key %}
      {% if unique_key is sequence and unique_key is not mapping and unique_key is not string %}
          {% for key in unique_key %}
              {% set this_key_match %}
                  DBT_INTERNAL_SOURCE.{{ key }} = DBT_INTERNAL_DEST.{{ key }}
              {% endset %}
              {% do predicates.append(this_key_match) %}
          {% endfor %}
      {% else %}
          {% set unique_key_match %}
              DBT_INTERNAL_SOURCE.{{ unique_key }} = DBT_INTERNAL_DEST.{{ unique_key }}
          {% endset %}
          {% do predicates.append(unique_key_match) %}
      {% endif %}
  {% else %}
      {% do predicates.append('FALSE') %}
  {% endif %}

  merge into {{ target }} as DBT_INTERNAL_DEST
    using {{ source }} as DBT_INTERNAL_SOURCE
    on {{"(" ~ predicates | join(") and (") ~ ")"}}

  {% if unique_key %}
    when matched then update set
      {% for column_name in update_columns -%}
          {{ column_name }} = DBT_INTERNAL_SOURCE.{{ column_name }}
          {%- if not loop.last %}, {%- endif %}
      {%- endfor %}
  {% endif %}

  when not matched then insert
    ({{ get_qualified_columnnames_csv(dest_columns) }})
  values
    ({{ get_qualified_columnnames_csv(dest_columns, 'DBT_INTERNAL_SOURCE') }})

{% endmacro %}


{% macro dbt_odps_get_incremental_sql(strategy, source, target, unique_key, dest_columns, sql) %}


  {%- if strategy == 'append' -%}
    {#-- insert new records into existing table, without updating or overwriting #}
    {{ get_insert_into_sql(source, target, dest_columns) }}
  {%- elif strategy == 'insert_overwrite' -%}
    {#-- insert statements don't like CTEs, so support them via a temp view #}
    {{ get_insert_overwrite_sql(source, target, sql) }}
  {%- elif strategy == 'merge' -%}
  {#-- merge all columns with databricks delta - schema changes are handled for us #}
    {{ get_merge_sql(target, source, unique_key, dest_columns, predicates=none) }}
  {%- else -%}
    {% set no_sql_for_strategy_msg -%}
      No known SQL for the incremental strategy provided: {{ strategy }}
    {%- endset %}
    {%- do exceptions.raise_compiler_error(no_sql_for_strategy_msg) -%}
  {%- endif -%}

{% endmacro %}


{% macro odps__get_incremental_default_sql(arg_dict) %}
  {#-- default mode is append, so return the sql for the same  #}
  {% do return(get_insert_into_sql(arg_dict["source_relation"], arg_dict["target_relation"], arg_dict["dest_columns"])) %}
{% endmacro %}

{% macro odps__assert_columns_equals(source_columns, target_columns) %}

  {% set schema_changed = False %}

  {%- set source_not_in_target = diff_columns(source_columns, target_columns) -%}
  {%- set target_not_in_source = diff_columns(target_columns, source_columns) -%}

  {% set new_target_types = diff_column_data_types(source_columns, target_columns) %}

  {%- if source_not_in_target != [] -%}
    {% set schema_changed = True %}
  {%- elif target_not_in_source != [] or new_target_types != [] -%}
    {% set schema_changed = True %}
  {%- elif new_target_types != [] -%}
    {% set schema_changed = True %}
  {%- endif -%}
  {%- if schema_changed -%}
  {% set fail_msg %}
      The source and target schemas on this incremental model are out of sync!
      They can be reconciled in several ways:
        - set the `on_schema_change` config to either append_new_columns or sync_all_columns, depending on your situation.
        - Re-run the incremental model with `full_refresh: True` to update the target schema.
        - update the schema manually and re-run the process.

      Additional troubleshooting context:
         Source columns not in target: {{ source_not_in_target }}
         Target columns not in source: {{ target_not_in_source }}
         New column types: {{ new_target_types }}
  {% endset %}

  {% do exceptions.raise_compiler_error(fail_msg) %}
  {%- endif -%}
{% endmacro %}