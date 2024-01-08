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

{% macro odps__dateadd(datepart, interval, from_date_or_timestamp) %}
    {%- if datepart in ['day', 'month', 'year'] %}
       dateadd({{ from_date_or_timestamp }}, {{ interval }}, '{{ datepart }}')
    {%- elif datepart == 'hour' -%}
       from_unixtime(unix_timestamp({{from_date_or_timestamp}}) + {{interval}}*3600)
    {%- else -%}
       {{ exceptions.raise_compiler_error("macro dateadd not implemented for datepart ~ '" ~ datepart ~ "' ~ on ODPS") }}
    {%- endif -%}
{% endmacro %}
