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

{% macro odps__snapshot_hash_arguments(args) -%}
    md5({%- for arg in args -%}
        coalesce(cast({{ arg }} as string ), '')
        {% if not loop.last %} || '|' || {% endif %}
    {%- endfor -%})
{%- endmacro %}


{% macro odps__snapshot_string_as_time(timestamp) -%}
    {%- set result = "to_timestamp('" ~ timestamp ~ "')" -%}
    {{ return(result) }}
{%- endmacro %}


{% macro odps__snapshot_merge_sql(target, source, insert_cols) -%}
    {%- set insert_cols_csv = insert_cols | join(', ') -%}

    insert overwrite table {{ target}} 
    select 
        {% for column in insert_cols -%}
            {%- if 'dbt_valid_to' in column  -%}
                case when  DBT_INTERNAL_SOURCE.dbt_change_type   in ('update', 'delete')
                           and DBT_INTERNAL_TARGET.dbt_valid_to is null
                          then     DBT_INTERNAL_SOURCE.{{ column }} 
                     else  DBT_INTERNAL_TARGET.{{ column }} 
                     end dbt_valid_to 
            {%- else -%}
                 DBT_INTERNAL_TARGET.{{ column }}  
            {%- endif -%}
            {%- if not loop.last -%}, {%- endif -%}
        {%- endfor %}
    from {{ target }} as  DBT_INTERNAL_TARGET
    left join {{ source }} as DBT_INTERNAL_SOURCE
    on DBT_INTERNAL_TARGET.dbt_scd_id = DBT_INTERNAL_SOURCE.dbt_scd_id
    union all 
    select 
      {% for column in insert_cols -%}
        DBT_INTERNAL_SOURCE.{{ column }} 
        {%- if not loop.last %}, {%- endif %}
      {%- endfor %}
    from {{ source }} as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_change_type = 'insert';

{% endmacro %}


{% macro odps_build_snapshot_staging_table(strategy, sql, target_relation) %}
    {% set tmp_identifier = target_relation.identifier ~ '__dbt_tmp' %}

    {%- set tmp_relation = api.Relation.create(identifier=tmp_identifier,
                                                  schema=target_relation.schema,
                                                  database=target_relation.database,
                                                  type='view') -%}

    {% set select = snapshot_staging_table(strategy, sql, target_relation) %}

    {# needs to be a non-temp view so that its columns can be ascertained via `describe` #}
    {% call statement('build_snapshot_staging_relation') %}
        {{ create_view_as(tmp_relation, select) }}
    {% endcall %}

    {% do return(tmp_relation) %}
{% endmacro %}


{% macro odps__post_snapshot(staging_relation) %}
    {% do adapter.drop_relation(staging_relation) %}
{% endmacro %}


{% macro odps__create_columns(relation, columns) %}
    {% if columns|length > 0 %}
    {% call statement() %}
      alter table {{ relation }} add columns (
        {% for column in columns %}
          `{{ column.name }}` {{ column.data_type }} {{- ',' if not loop.last -}}
        {% endfor %}
      );
    {% endcall %}
    {% endif %}
{% endmacro %}


{% materialization snapshot, adapter='odps' %}
  {%- set config = model['config'] -%}

  {%- set target_table = model.get('alias', model.get('name')) -%}

  {%- set strategy_name = config.get('strategy') -%}
  {%- set unique_key = config.get('unique_key') %}

  {% set target_relation_exists, target_relation = get_or_create_relation(
          database=model.database,
          schema=model.schema,
          identifier=target_table,
          type='table') -%}


  

  {% if not adapter.check_schema_exists(model.database, model.schema) %}
    {% do create_schema(model.database, model.schema) %}
  {% endif %}

  {%- if not target_relation.is_table -%}
    {% do exceptions.relation_wrong_type(target_relation, 'table') %}
  {%- endif -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% set strategy_macro = strategy_dispatch(strategy_name) %}
  {% set strategy = strategy_macro(model, "snapshotted_data", "source_data", config, target_relation_exists) %}

  {% if not target_relation_exists %}

      {% set build_sql = build_snapshot_table(strategy, model['compiled_sql']) %}
      {% set final_sql = create_table_as(False, target_relation, build_sql) %}

  {% else %}

      {{ adapter.valid_snapshot_target(target_relation) }}

      {% set staging_table = odps_build_snapshot_staging_table(strategy, sql, target_relation) %}

      -- this may no-op if the database does not require column expansion
      {% do adapter.expand_target_column_types(from_relation=staging_table,
                                               to_relation=target_relation) %}

      {% set missing_columns = adapter.get_missing_columns(staging_table, target_relation)
                                   | rejectattr('name', 'equalto', 'dbt_change_type')
                                   | rejectattr('name', 'equalto', 'DBT_CHANGE_TYPE')
                                   | rejectattr('name', 'equalto', 'dbt_unique_key')
                                   | rejectattr('name', 'equalto', 'DBT_UNIQUE_KEY')
                                   | list %}

      {% do create_columns(target_relation, missing_columns) %}

      {% set dest_columns = adapter.get_columns_in_relation(target_relation)
                                   | rejectattr('name', 'equalto', 'dbt_change_type')
                                   | rejectattr('name', 'equalto', 'DBT_CHANGE_TYPE')
                                   | rejectattr('name', 'equalto', 'dbt_unique_key')
                                   | rejectattr('name', 'equalto', 'DBT_UNIQUE_KEY')
                                   | list %}

      {% set quoted_dest_columns = [] %}
      {% for column in dest_columns %}
        {% do quoted_dest_columns.append(adapter.quote(column.name)) %}
      {% endfor %}

      {% set final_sql = snapshot_merge_sql(
            target = target_relation,
            source = staging_table,
            insert_cols = quoted_dest_columns,
            
         )
      %}

  {% endif %}

  {% call statement('main') %}
      {{ final_sql }}
  {% endcall %}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {{ adapter.commit() }}

  {% if staging_table is defined %}
      {% do post_snapshot(staging_table) %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
