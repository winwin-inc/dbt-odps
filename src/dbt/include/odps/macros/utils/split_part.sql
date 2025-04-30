
{% macro odps__split_part(string_text, delimiter_text, part_number) %}
    {% if part_number > 0 -%}
        split_part(
            {{ string_text }},
            {{ delimiter_text }},
            {{ part_number }}
            )
    {%- else -%}
        split_part(
            {{ string_text }},
            {{ delimiter_text }},
              length({{ string_text }})
              - length(
                  replace({{ string_text }},  {{ delimiter_text }}, '')
              ) + 2 + {{ part_number }}
            )
    {%- endif -%}
{% endmacro %}

{% macro _split_part_negative(string_text, delimiter_text, part_number) %}

    split_part(
        {{ string_text }},
        {{ delimiter_text }},
          length({{ string_text }})
          - length(
              replace({{ string_text }},  {{ delimiter_text }}, '')
          ) + 2 + {{ part_number }}
        )

{% endmacro %}
