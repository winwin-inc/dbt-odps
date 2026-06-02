-- only change varchar to string, dbt-adapters/dbt/include/global_project/macros/materializations/snapshots/strategies.sql
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
    {%- set columns = config.get('snapshot_table_column_names') or get_snapshot_table_column_names() -%}
    {%- set hard_deletes = config.get('hard_deletes', 'ignore') -%}
    {%- set dbt_valid_to_current = config.get('dbt_valid_to_current') -%}

    insert overwrite table {{ target }}
    select
        {% for column in insert_cols -%}
            {%- set _col_name = column | replace('`', '') -%}
            {%- if _col_name == columns.dbt_valid_to -%}
                case when DBT_INTERNAL_SOURCE.dbt_change_type in ('update', 'delete')
                         {%- if dbt_valid_to_current %}
                         and ( DBT_INTERNAL_TARGET.{{ adapter.quote(columns.dbt_valid_to) }} = {{ dbt_valid_to_current }} or
                               DBT_INTERNAL_TARGET.{{ adapter.quote(columns.dbt_valid_to) }} is null )
                         {%- else %}
                         and DBT_INTERNAL_TARGET.{{ adapter.quote(columns.dbt_valid_to) }} is null
                         {%- endif %}
                     then DBT_INTERNAL_SOURCE.{{ column }}
                     else DBT_INTERNAL_TARGET.{{ column }}
                     end {{ columns.dbt_valid_to }}
            {%- else -%}
                 DBT_INTERNAL_TARGET.{{ column }}
            {%- endif -%}
            {%- if not loop.last -%}, {%- endif -%}
        {%- endfor %}
    from {{ target }} as DBT_INTERNAL_TARGET
    left join (
        {{ source }}
    ) as DBT_INTERNAL_SOURCE
    on DBT_INTERNAL_TARGET.{{ adapter.quote(columns.dbt_scd_id) }} = DBT_INTERNAL_SOURCE.{{ adapter.quote(columns.dbt_scd_id) }}
    union all
    select
      {% for column in insert_cols -%}
        DBT_INTERNAL_SOURCE.{{ column }}
        {%- if not loop.last %}, {%- endif %}
      {%- endfor %}
    from (
        {{ source }}
    ) as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_change_type = 'insert';

{% endmacro %}


{% macro odps_build_snapshot_staging_table(strategy, sql, target_relation) %}
    {% set columns = config.get('snapshot_table_column_names') or get_snapshot_table_column_names() %}
    {% if strategy.hard_deletes == 'new_record' %}
        {% set new_scd_id = snapshot_hash_arguments([columns.dbt_scd_id, snapshot_get_time()]) %}
    {% endif %}
    {% set staging_sql %}
    with snapshot_query as (

        {{ sql }}

    ),

    snapshotted_data as (

        select *, {{ unique_key_fields(strategy.unique_key) }}
        from {{ target_relation }}
        where
            {% if config.get('dbt_valid_to_current') %}
                {% set source_unique_key = columns.dbt_valid_to | trim %}
                {% set target_unique_key = config.get('dbt_valid_to_current') | trim %}

                ( {{ equals(source_unique_key, target_unique_key) }} or {{ source_unique_key }} is null )
            {% else %}
                {{ columns.dbt_valid_to }} is null
            {% endif %}

    ),

    insertions_source_data as (

        select *, {{ unique_key_fields(strategy.unique_key) }},
            {{ strategy.updated_at }} as {{ columns.dbt_updated_at }},
            {{ strategy.updated_at }} as {{ columns.dbt_valid_from }},
            {{ get_dbt_valid_to_current(strategy, columns) }},
            {{ strategy.scd_id }} as {{ columns.dbt_scd_id }}

        from snapshot_query
    ),

    updates_source_data as (

        select *, {{ unique_key_fields(strategy.unique_key) }},
            {{ strategy.updated_at }} as {{ columns.dbt_updated_at }},
            {{ strategy.updated_at }} as {{ columns.dbt_valid_from }},
            {{ strategy.updated_at }} as {{ columns.dbt_valid_to }}

        from snapshot_query
    ),

    {%- if strategy.hard_deletes == 'invalidate' or strategy.hard_deletes == 'new_record' %}

    deletes_source_data as (

        select *, {{ unique_key_fields(strategy.unique_key) }}
        from snapshot_query
    ),
    {% endif %}

    insertions as (

        select
            'insert' as dbt_change_type,
            source_data.*
            {%- if strategy.hard_deletes == 'new_record' -%}
            , false as {{ columns.dbt_is_deleted }}
            {%- endif %}

        from insertions_source_data as source_data
        left outer join snapshotted_data
            on {{ unique_key_join_on(strategy.unique_key, "snapshotted_data", "source_data") }}
            where {{ unique_key_is_null(strategy.unique_key, "snapshotted_data") }}
            or ({{ unique_key_is_not_null(strategy.unique_key, "snapshotted_data") }} and (
                {{ strategy.row_changed }} {%- if strategy.hard_deletes == 'new_record' -%} or snapshotted_data.{{ columns.dbt_is_deleted }} = true {% endif %}
            )

        )

    ),

    updates as (

        select
            'update' as dbt_change_type,
            source_data.*,
            snapshotted_data.{{ columns.dbt_scd_id }}
            {%- if strategy.hard_deletes == 'new_record' -%}
            , snapshotted_data.{{ columns.dbt_is_deleted }}
            {%- endif %}

        from updates_source_data as source_data
        join snapshotted_data
            on {{ unique_key_join_on(strategy.unique_key, "snapshotted_data", "source_data") }}
        where (
            {{ strategy.row_changed }} {%- if strategy.hard_deletes == 'new_record' -%} or snapshotted_data.{{ columns.dbt_is_deleted }} = true {% endif %}
        )
    )

    {%- if strategy.hard_deletes == 'invalidate' or strategy.hard_deletes == 'new_record' %}
    ,
    deletes as (

        select
            'delete' as dbt_change_type,
            source_data.*,
            (select {{ strategy.updated_at }} from snapshot_query limit 1) as {{ columns.dbt_updated_at }},
            (select {{ strategy.updated_at }} from snapshot_query limit 1) as {{ columns.dbt_valid_from }},
            (select {{ strategy.updated_at }} from snapshot_query limit 1) as {{ columns.dbt_valid_to }},
            snapshotted_data.{{ columns.dbt_scd_id }}
            {%- if strategy.hard_deletes == 'new_record' -%}
            , snapshotted_data.{{ columns.dbt_is_deleted }}
            {%- endif %}
        from snapshotted_data
        left join deletes_source_data as source_data
            on {{ unique_key_join_on(strategy.unique_key, "snapshotted_data", "source_data") }}
        where {{ unique_key_is_null(strategy.unique_key, "source_data") }}

            {%- if strategy.hard_deletes == 'new_record' %}
            and not (
                snapshotted_data.{{ columns.dbt_is_deleted }} = true
                and
                {% if config.get('dbt_valid_to_current') -%}
                    snapshotted_data.{{ columns.dbt_valid_to }} = {{ config.get('dbt_valid_to_current') }}
                {%- else -%}
                    snapshotted_data.{{ columns.dbt_valid_to }} is null
                {%- endif %}
            )
            {%- endif %}
    )
    {%- endif %}

    {%- if strategy.hard_deletes == 'new_record' %}
        {% set target_columns_raw = get_columns_in_relation(target_relation) %}
        {% set snapshotted_cols = [] %}
        {% for row in target_columns_raw %}
            {% set col_name = row[0] %}
            {% if col_name and not col_name.startswith('#') %}
                {% do snapshotted_cols.append(col_name) %}
            {% endif %}
        {% endfor %}
        {% set source_sql_cols = get_column_schema_from_query(sql) %}
    ,
    deletion_records as (

        select
            'insert' as dbt_change_type,
            {%- for col in source_sql_cols -%}
            {%- if col.name in snapshotted_cols -%}
            snapshotted_data.{{ adapter.quote(col.column) }},
            {%- else -%}
            NULL as {{ adapter.quote(col.column) }},
            {%- endif -%}
            {% endfor -%}
            {%- if strategy.unique_key | is_list -%}
                {%- for key in strategy.unique_key -%}
            snapshotted_data.{{ key }} as dbt_unique_key_{{ loop.index }},
                {% endfor -%}
            {%- else -%}
            snapshotted_data.dbt_unique_key as dbt_unique_key,
            {% endif -%}
            (select {{ strategy.updated_at }} from snapshot_query limit 1) as {{ columns.dbt_updated_at }},
            (select {{ strategy.updated_at }} from snapshot_query limit 1) as {{ columns.dbt_valid_from }},
            snapshotted_data.{{ columns.dbt_valid_to }} as {{ columns.dbt_valid_to }},
            {{ new_scd_id }} as {{ columns.dbt_scd_id }},
            true as {{ columns.dbt_is_deleted }}
        from snapshotted_data
        left join deletes_source_data as source_data
            on {{ unique_key_join_on(strategy.unique_key, "snapshotted_data", "source_data") }}
        where {{ unique_key_is_null(strategy.unique_key, "source_data") }}
        and not (
            snapshotted_data.{{ columns.dbt_is_deleted }} = true
            and
            {% if config.get('dbt_valid_to_current') -%}
                snapshotted_data.{{ columns.dbt_valid_to }} = {{ config.get('dbt_valid_to_current') }}
            {%- else -%}
                snapshotted_data.{{ columns.dbt_valid_to }} is null
            {%- endif %}
            )

    )
    {%- endif %}

    select * from insertions
    union all
    select * from updates
    {%- if strategy.hard_deletes == 'invalidate' or strategy.hard_deletes == 'new_record' %}
    union all
    select * from deletes
    {%- endif %}
    {%- if strategy.hard_deletes == 'new_record' %}
    union all
    select * from deletion_records
    {%- endif %}
    {% endset %}

    {{ return(staging_sql) }}
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


{% macro odps__build_snapshot_table(strategy, sql) %}
    {%- set columns = config.get("snapshot_table_column_names") or get_snapshot_table_column_names() -%}
    {%- set hard_deletes = strategy.hard_deletes -%}

    select *,
        {{ strategy.scd_id }} as {{ columns.dbt_scd_id }},
        {{ strategy.updated_at }} as {{ columns.dbt_updated_at }},
        {{ strategy.updated_at }} as {{ columns.dbt_valid_from }},
        {{ get_dbt_valid_to_current(strategy, columns) }}
        {%- if hard_deletes == 'new_record' -%}
        , false as {{ columns.dbt_is_deleted }}
        {%- endif %}
    from (
        {{ sql }}
    ) sbq
{% endmacro %}



{# ─── Standard snapshot materialization ─── #}

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

      {% set build_sql = build_snapshot_table(strategy, model['compiled_code']) %}
      {% set final_sql = create_table_as(False, target_relation, build_sql) %}

  {% else %}

      {% set columns = config.get("snapshot_table_column_names") or get_snapshot_table_column_names() %}

      {{ adapter.assert_valid_snapshot_target_given_strategy(target_relation, columns, strategy) }}

      {# Generate staging SQL directly, no intermediate view #}
      {% set build_or_select_sql = odps_build_snapshot_staging_table(strategy, sql, target_relation) %}

      {% set remove_columns = ['dbt_change_type', 'DBT_CHANGE_TYPE', 'dbt_unique_key', 'DBT_UNIQUE_KEY'] %}
      {% if unique_key | is_list %}
          {% for key in strategy.unique_key %}
              {{ remove_columns.append('dbt_unique_key_' + loop.index|string) }}
              {{ remove_columns.append('DBT_UNIQUE_KEY_' + loop.index|string) }}
          {% endfor %}
      {% endif %}

      {% set dest_columns = adapter.get_columns_in_relation(target_relation)
                                   | rejectattr('name', 'in', remove_columns)
                                   | list %}

      {% set quoted_dest_columns = [] %}
      {% for column in dest_columns %}
        {% do quoted_dest_columns.append(adapter.quote(column.name)) %}
      {% endfor %}

      {% set final_sql = snapshot_merge_sql(
            target = target_relation,
            source = build_or_select_sql,
            insert_cols = quoted_dest_columns
         )
      %}

  {% endif %}

  {% call statement('main') %}
      {{ final_sql }}
  {% endcall %}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {{ adapter.commit() }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
