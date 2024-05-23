{% macro file_format_clause() %}
  {%- set file_format = config.get('file_format', validator=validation.any[basestring]) -%}
  {%- if file_format is not none %}
    stored as {{ file_format }}
  {%- endif %}
{%- endmacro -%}

{% macro location_clause() %}
  {%- set location_root = config.get('location_root', validator=validation.any[basestring]) -%}
  {%- set identifier = model['alias'] -%}
  {%- if location_root is not none %}
    location '{{ location_root }}/{{ identifier }}'
  {%- endif %}
{%- endmacro -%}

{% macro options_clause() -%}
  {%- set options = config.get('options') -%}
  {%- if options is not none %}
    options (
      {%- for option in options -%}
      {{ option }} "{{ options[option] }}" {% if not loop.last %}, {% endif %}
      {%- endfor %}
    )
  {%- endif %}
{%- endmacro -%}

{% macro comment_clause() %}
  {%- set raw_persist_docs = config.get('persist_docs', {}) -%}

  {%- if raw_persist_docs is mapping -%}
    {%- set raw_relation = raw_persist_docs.get('relation', false) -%}
      {%- if raw_relation -%}
      comment '{{ model.description | replace("'", "\\'") }}'
      {% endif %}
  {%- else -%}
    {{ exceptions.raise_compiler_error("Invalid value provided for 'persist_docs'. Expected dict but got value: " ~ raw_persist_docs) }}
  {% endif %}
{%- endmacro -%}

{% macro comment_clause_ignore() %}
   {{ "comment" }} '{{ model.description | replace("'", "\\'") }}'
{%- endmacro -%}

{% macro lifecycle_clause(temporary) %}
  {%- set lifecycle = config.get('lifecycle') -%}
  {%- if lifecycle is not none -%}
    lifecycle {{ lifecycle }}
  {%- elif temporary -%}
    lifecycle 1
  {%- endif %}
{%- endmacro -%}

{% macro properties_clause() %}
  {%- set properties = config.get('tbl_properties') -%}
  {%- if properties is not none -%}
      TBLPROPERTIES (
          {%- for key, value in properties.items() -%}
            "{{ key }}" = "{{ value }}"
            {%- if not loop.last -%}{{ ',\n  ' }}{%- endif -%}
          {%- endfor -%}
      )
  {%- endif -%}
{%- endmacro -%}

{% macro stored_by_clause(table_type) %}
  {%- if table_type is not none %}
    stored by {{ table_type }}
  {%- endif %}
{%- endmacro -%}

{% macro partition_cols(label, required=false) %}
  {%- set cols = config.get('partition_by', validator=validation.any[list, basestring]) -%}
  {%- if cols is not none %}
    {%- if cols is mapping  -%}
      {%- set cols = [cols] -%}
    {%- endif -%}
    {{ label }} (
    {%- for item in cols -%}
      {{ item.field }}
      {%- if not loop.last -%},{%- endif -%}
    {%- endfor -%}
    )
  {%- endif %}
{%- endmacro -%}


{% macro partition_clause() %}
  {%- set cols = config.get('partition_by', validator=validation.any[list, basestring]) -%}
  {%- if cols is not none %}
    {%- if cols is mapping  -%}
      {%- set cols = [cols] -%}
    {%- endif -%}
    partitioned by (
    {%- for item in cols -%}
      {{ item.field }} {{ item.data_type }}{%- if item.comment %} comment '{{ item.comment }}'{%- endif -%}
      {%- if not loop.last -%},{%- endif -%}
    {%- endfor -%}
    )
  {%- endif %}
{%- endmacro -%}


{% macro clustered_cols(label, required=false) %}
  {%- set cols = config.get('clustered_by', validator=validation.any[list, basestring]) -%}
  {%- set buckets = config.get('buckets', validator=validation.any[int]) -%}
  {%- if (cols is not none) and (buckets is not none) %}
    {%- if cols is string -%}
      {%- set cols = [cols] -%}
    {%- endif -%}
    {{ label }} (
    {%- for item in cols -%}
      {{ item }}
      {%- if not loop.last -%},{%- endif -%}
    {%- endfor -%}
    ) into {{ buckets }} buckets
  {%- endif %}
{%- endmacro -%}

{% macro fetch_tbl_properties(relation) -%}
  {% call statement('list_properties', fetch_result=True) -%}
    SHOW TBLPROPERTIES {{ relation }}
  {% endcall %}
  {% do return(load_result('list_properties').table) %}
{%- endmacro %}

{% macro create_temporary_view(relation, sql) -%}
  --  We can't use temporary tables with `create ... as ()` syntax in Hive2
  -- create temporary view {{ relation.include(schema=false) }} as
  create temporary table {{ relation.include(schema=false) }} as
    {{ sql }}
{% endmacro %}

{% macro odps__create_table_as(temporary, relation, sql) -%}
 
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}



  {% set is_external = config.get('external') -%}
  {%- set table_type = config.get('table_type') -%}
  {%- if temporary -%}
    {{ create_temporary_view(relation, sql) }}
  {%- else -%}
    {% set contract_config = config.get('contract') %}
    {% if contract_config.enforced %}
      {# set odps.sql.submit.mode='script'; #}
      {% call statement('create_table', auto_begin=False) -%}
          create {% if is_external == true -%}external{%- endif %} table {{ relation }}
          {{ get_table_columns_and_constraints() }}
          {{ options_clause() }}
          {{ partition_clause() }}
          {{ comment_clause_ignore() }}
          {{ clustered_cols(label="clustered by") }}
          {{ stored_by_clause(table_type) }}
          {{ file_format_clause() }}
          {{ location_clause() }}
          {{ comment_clause() }}
          {{ properties_clause() }}
          {{ lifecycle_clause(temporary) }}
          ;
      {% endcall %}

      {{ get_assert_columns_equivalent(sql) }}
      {%- set sql = get_select_subquery(sql) %}
      insert into {{ relation }} {{ partition_cols(label="partition") }}
      (
          {{ sql }}
      );
    {% elif config.get('partition_by') %}
      {# set odps.sql.submit.mode='script'; #}
       {% call statement('create_table', auto_begin=False) -%}

      create {% if is_external == true -%}external{%- endif %} table {{ relation }}
      {{ odps__get_table_columns_and_constraints_from_query(sql) }}
      {{ options_clause() }}
      {{ comment_clause_ignore() }}
      {{ partition_clause() }}
      {{ clustered_cols(label="clustered by") }}
      {{ stored_by_clause(table_type) }}
      {{ file_format_clause() }}
      {{ location_clause() }}
      {{ comment_clause() }}
      {{ properties_clause() }}
      {{ lifecycle_clause(temporary) }}
      ;

      {% endcall %}
      insert into {{ relation }} {{ partition_cols(label="partition") }}
      (
          {{ sql }}
      );
    {%- else -%}
      create table {{ relation }}
      {{ lifecycle_clause(temporary) }}
      as {{ sql }}
    {%- endif %}
  {%- endif %}
{%- endmacro %}

{% macro odps__create_view_as(relation, sql) -%}
  create or replace view {{ relation }}
    {% set contract_config = config.get('contract') %}
    {% if contract_config.enforced %}
      {{ get_assert_columns_equivalent(sql) }}
    {%- endif %}
  as {{ sql }};
{%- endmacro %}

{% macro odps__drop_relation(relation) -%}
  {% call statement('drop_relation', auto_begin=False) -%}
    drop {{ relation.type }} if exists {{ relation }}
  {%- endcall %}
{% endmacro %}

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

{% macro show_create_table(relation) %}
  {% call statement('show_create_table', fetch_result=True) -%}
    show create table {{ relation }}
  {%- endcall %}

  {% set result = load_result('show_create_table') %}
  {% do return(result.table[0][0]) %}
{% endmacro %}


{% macro odps__list_tables_without_caching(schema) %}
  {% call statement('list_tables_without_caching', fetch_result=True) -%}
    show tables  
  {% endcall %}
  {% do return(load_result('list_tables_without_caching').table) %}
{% endmacro %}

{% macro odps__list_views_without_caching(schema) %}
  {% call statement('list_views_without_caching', fetch_result=True) -%}
    select table_name from information_schema.TABLES where table_type = 'VIRTUAL_VIEW'
  {% endcall %}
  {% do return(load_result('list_views_without_caching').table) %}
{% endmacro %}

{% macro odps__get_columns_from_query(sql) %}
{%- set partition_cols = config.get('partition_by', validator=validation.any[list, basestring]) -%}
  {%- set partition_col_names = [] -%}
  {%- if partition_cols is not none %}
    {%- if partition_cols is mapping  -%}
    {%- set partition_cols = [partition_cols] -%}
    {%- endif -%}
    {%- for item in partition_cols -%}
    {%- do partition_col_names.append(item.field) -%}
    {%- endfor -%}
  {%- endif %}

  {#-- Obtain the column schema provided by sql file. #}
  {%- set sql_file_provided_columns = get_column_schema_from_query(sql, config.get('sql_header', none)) -%}
  {%- set columns = [] -%}
  {%- for c in sql_file_provided_columns -%}
    {%- if c.name not in partition_col_names -%}
      {%- do columns.append(c) -%}
    {%- endif -%}
  {%- endfor -%}
  {% do return(columns) %}
{% endmacro %}

{% macro odps__get_table_columns_and_constraints_from_query(sql) -%}
(
    {% set model_columns = model.columns %}
    {% for c in odps__get_columns_from_query(sql) -%}
    {{ c.name }} {{ c.dtype }} 
    {% if model_columns and c.name in  model_columns -%}
       {{ "COMMENT" }} '{{ model_columns[c.name].description }}' 
    {%- endif %}
    {{ "," if not loop.last or raw_model_constraints }}

    {% endfor %}
)
{%- endmacro %}

{% macro odps__get_assert_columns_equivalent(sql) -%}
  {{ return(odps_assert_columns_equivalent(sql)) }}
{%- endmacro %}

{#
  Compares the column schema provided by a model's sql file to the column schema provided by a model's schema file.
  If any differences in name, data_type or number of columns exist between the two schemas, raises a compiler error
#}
{% macro odps_assert_columns_equivalent(sql) %}

  {#-- First ensure the user has defined 'columns' in yaml specification --#}
  {%- set user_defined_columns = model['columns'] -%}
  {%- if not user_defined_columns -%}
      {{ exceptions.raise_contract_error([], []) }}
  {%- endif -%}
  {%- set partition_cols = config.get('partition_by', validator=validation.any[list, basestring]) -%}
  {%- if partition_cols is not none %}
    {%- if partition_cols is mapping  -%}
    {%- set partition_cols = [partition_cols] -%}
    {%- endif -%}
    {%- for item in partition_cols -%}
    {%- do user_defined_columns.update({item.field: {'name': item.field, 'data_type': item.data_type}}) -%}
    {%- endfor -%}
  {%- endif %}

  {#-- Obtain the column schema provided by sql file. #}
  {%- set sql_file_provided_columns = get_column_schema_from_query(sql, config.get('sql_header', none)) -%}
  {#--Obtain the column schema provided by the schema file by generating an 'empty schema' query from the model's columns. #}
  {%- set schema_file_provided_columns = get_column_schema_from_query(get_empty_schema_sql(user_defined_columns)) -%}

  {#-- create dictionaries with name and formatted data type and strings for exception #}
  {%- set sql_columns = format_columns(sql_file_provided_columns) -%}
  {%- set yaml_columns = format_columns(schema_file_provided_columns)  -%}

  {%- if sql_columns|length != yaml_columns|length -%}
    {%- do exceptions.raise_contract_error(yaml_columns, sql_columns) -%}
  {%- endif -%}

  {%- for sql_col in sql_columns -%}
    {%- set yaml_col = [] -%}
    {%- for this_col in yaml_columns -%}
      {%- if this_col['name'] == sql_col['name'] -%}
        {%- do yaml_col.append(this_col) -%}
        {%- break -%}
      {%- endif -%}
    {%- endfor -%}
    {%- if not yaml_col -%}
      {#-- Column with name not found in yaml #}
      {%- do exceptions.raise_contract_error(yaml_columns, sql_columns) -%}
    {%- endif -%}
    {%- if sql_col['formatted'] != yaml_col[0]['formatted'] -%}
      {#-- Column data types don't match #}
      {%- do exceptions.raise_contract_error(yaml_columns, sql_columns) -%}
    {%- endif -%}
  {%- endfor -%}

{%- endmacro %}