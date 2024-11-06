

models_simple_sql = """
 {{ config(materialized = "table")}}

select branch_id
from {{ source('dim','view_public_branch')}}
            where ds = 20241001 
"""

models_simple_schema_yml = """version: 2

models:
  - name: models_simple
    columns:
      - name: branch_id

sources:
  - name: dim
    database: zhidou_hz_dev 
    schema: default
    tables:
      - name: view_public_branch 


"""


macros_sql = """
{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- else -%}

        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}

"""
