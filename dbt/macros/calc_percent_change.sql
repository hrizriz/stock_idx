{% macro calc_percent_change(current, previous) %}
  CASE
    WHEN {{ previous }} != 0 THEN ({{ current }} - {{ previous }}) / {{ previous }} * 100
    ELSE 0
  END
{% endmacro %}