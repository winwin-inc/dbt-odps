{% macro mc_generate_incremental_insert_overwrite_build_sql(
    tmp_relation, target_relation, sql, unique_key, partition_by, partitions, dest_columns, tmp_relation_exists
) %}
    {% if partition_by is none %}
      {% set missing_partition_msg -%}
      The 'insert_overwrite' strategy requires the `partition_by` config.
      {%- endset %}
      {% do exceptions.raise_compiler_error(missing_partition_msg) %}
    {% endif %}

    {% if partition_by.fields|length != 1 %}
      {% set missing_partition_msg -%}
      The 'insert_overwrite' strategy requires the `partition_by` config.
      {%- endset %}
      {% do exceptions.raise_compiler_error(missing_partition_msg) %}
    {% endif %}

    {% set build_sql = mc_insert_overwrite_sql(
        tmp_relation, target_relation, sql, unique_key, partition_by, partitions, dest_columns, tmp_relation_exists
    ) %}

    {{ return(build_sql) }}

{% endmacro %}

{% macro mc_insert_overwrite_sql(
    tmp_relation, target_relation, sql, unique_key, partition_by, partitions, dest_columns, tmp_relation_exists
) %}
      {% if not tmp_relation_exists %}
        {%- call statement('create_tmp_relation') -%}
          {{ create_table_as_internal(True, tmp_relation, sql, True, partition_config=partition_by) }}
        {%- endcall -%}
      {% endif %}
      -- 3. run the merge statement
      {%- call statement('main') -%}
      {% if partitions is not none and partitions != [] %} {# static #}
          {{ mc_static_insert_overwrite_merge_sql(target_relation, tmp_relation, partition_by, partitions) }}
      {% else %} {# dynamic #}
          {{ mc_dynamic_insert_overwrite_sql(target_relation, tmp_relation, partition_by) }}
      {% endif %}
      {%- endcall -%}
      -- 4. clean up the temp table
      drop table if exists {{ tmp_relation }}
{% endmacro %}

{% macro mc_static_insert_overwrite_merge_sql(target, source, partition_by, partitions) -%}
    {%- set sql_header = config.get('sql_header', none) -%}
    {{ sql_header if sql_header is not none and include_sql_header }}

    {%- call statement('drop_static_partition') -%}
    DELETE FROM {{ target }}
    WHERE {{ partition_by.render(False) }} in ({{ partitions | join(',') }})
    {%- endcall -%}

    INSERT OVERWRITE TABLE {{ target }} PARTITION({{ partition_by.render(False) }})
    (
    SELECT *
    FROM {{ source }}
    WHERE {{ partition_by.render(False) }} in ({{ partitions | join(',') }})
    )
{% endmacro %}

{% macro mc_dynamic_insert_overwrite_sql(target, source, partition_by) -%}
    {%- set sql_header = config.get('sql_header', none) -%}
    {{ sql_header if sql_header is not none and include_sql_header }}
    {% if partition_by.auto_partition() -%}
    INSERT OVERWRITE TABLE {{ target }}
    (
    SELECT *
    FROM {{ source }}
    )
    {%- else -%}
    INSERT OVERWRITE TABLE {{ target }} PARTITION({{ partition_by.render(False) }})
    (
    SELECT *
    FROM {{ source }}
    )
    {%- endif -%}
{% endmacro %}

{% macro odps_get_insert_overwrite_sql(source_relation, target_relation, sql, partition_by) %}
     {%- set sql_header = config.get('sql_header', none) %}
    {{ sql_header if sql_header is not none }}

    {%- set source_columns = odps_get_columns_from_query(sql, partition_by) -%}
    {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}

    {%- do log('source_columns: ' ~ source_columns|join(',') ) -%}
    {%- do log('dest_columns: ' ~ dest_columns|join(',') ) -%}

    {% do odps_assert_columns_equals(source_columns, dest_columns) %}
    insert overwrite table {{ target_relation }}
    PARTITION ({{ partition_by.render(False) }})
    {{ sql }}

{% endmacro %}

{% macro odps_get_columns_from_query(sql, partition_by) %}
  {#-- Obtain the column schema provided by sql file. #}
  {%- set sql_file_provided_columns = get_column_schema_from_query(sql, config.get('sql_header', none)) -%}
  {%- set columns = [] -%}
  {%- for c in sql_file_provided_columns -%}
    {%- if c.name not in partition_by.fields -%}
      {%- do columns.append(c) -%}
    {%- endif -%}
  {%- endfor -%}
  {% do return(columns) %}
{% endmacro %}


{% macro odps_assert_columns_equals(source_columns, target_columns) %}
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