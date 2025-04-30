{% macro parse_date(the_date) %}
{% do return(modules.date.parse_date(the_date)) %}
{% endmacro %}

{% macro add_months(the_date, months) %}
{% do return(parse_date(the_date).add_months(months)) %}
{% endmacro %}

{% macro add_weeks(the_date, weeks) %}
{% do return(parse_date(the_date).add_weeks(weeks)) %}
{% endmacro %}

{% macro add_days(the_date, days) %}
{% do return(parse_date(the_date).add_days(days)) %}
{% endmacro %}