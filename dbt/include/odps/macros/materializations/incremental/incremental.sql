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

{% materialization incremental, adapter='odps' %}

  -- relations
  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set temp_relation = make_temp_relation(target_relation)-%}

 {{ log("existing_relation " ~ this.database ~ ", identifier " ~ this.identifier ~ ",full_refresh_mode " ~ full_refresh_mode) }}

  -- configs
  {%- set unique_key = config.get('unique_key') -%}
  {%- set incremental_strategy = config.get("incremental_strategy") -%}
  {%- set full_refresh_mode = (should_full_refresh()  or existing_relation.is_view) -%}

  {% set existing_relation="existing_relation"  %}

  {% if existing_relation is none %}
      {% set build_sql = get_create_table_as_sql(False, target_relation, sql) %}
  {% elif full_refresh_mode %}
      {% set build_sql = get_create_table_as_sql(False, intermediate_relation, sql) %}
      {% set need_swap = true %}
  {% else %}

    {#-- Get the incremental_strategy, the macro to use for the strategy, and build the sql --#}
    {% set build_sql = dbt_hive_get_incremental_sql(incremental_strategy, temp_relation, target_relation, unique_key, dest_columns) %}}

  {% endif %}

  {% call statement("main") %}
      {{ build_sql }}
  {% endcall %}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
