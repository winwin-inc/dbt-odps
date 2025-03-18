
{% macro odps__get_catalog_relations(information_schema, schemas) -%}
  {%- call statement('catalog', fetch_result=True) -%}
    select
        tbl.table_schema as table_database,
        tbl.table_schema  as table_schema,
        tbl.table_name as table_name,
        case tbl.table_type
            when 'VIRTUAL_VIEW' then 'VIEW'
            else 'TABLE'
        end as table_type,
        tbl.table_comment as table_comment,
        col.column_name as column_name,
        col.ordinal_position as column_index,
        col.data_type as column_type,
        col.column_comment as column_comment,
        tbl.owner_name as table_owner
    from SYSTEM_CATALOG.information_schema.columns col
    join SYSTEM_CATALOG.information_schema.tables tbl
    on tbl.TABLE_CATALOG = col.TABLE_CATALOG
        and tbl.TABLE_SCHEMA = col.TABLE_SCHEMA
        and col.table_name = tbl.table_name
    order by
        tbl.table_name,
        col.ordinal_position

  {%- endcall -%}

  {{ return(load_result('catalog').table) }}
{%- endmacro %}


{% macro odps__get_catalog(information_schema, schemas) -%}
  {%- set relations = [] -%}
  {%- for schema in schemas -%}
    {%- set dummy = relations.append({'schema': schema}) -%}
  {%- endfor -%}
  {{ return(odps__get_catalog_relations(information_schema, relations)) }}
{%- endmacro %}


 