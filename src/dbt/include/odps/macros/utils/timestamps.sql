---------------------------------------------

/* {#
    DEPRECATED: DO NOT USE IN NEW PROJECTS

    This is ONLY to handle the fact that Snowflake + Postgres had functionally
    different implementations of {{ dbt.current_timestamp }} + {{ dbt_utils.current_timestamp }}

    If you had a project or package that called {{ dbt_utils.current_timestamp() }}, you should
    continue to use this macro to guarantee identical behavior on those two databases.
#} */

{% macro odps__current_timestamp_backcompat() %}
    current_timestamp()
{% endmacro %}

{% macro odps__current_timestamp_in_utc_backcompat() %}
    current_timestamp()
{% endmacro %}
