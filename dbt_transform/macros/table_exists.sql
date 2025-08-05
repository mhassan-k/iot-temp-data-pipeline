{% macro table_exists(schema_name, table_name) %}
  {% set query %}
    select count(*) as table_count 
    from information_schema.tables 
    where table_schema = '{{ schema_name }}' 
    and table_name = '{{ table_name }}'
  {% endset %}
  
  {% if execute %}
    {% set results = run_query(query) %}
    {{ return(results.columns[0].values()[0] > 0) }}
  {% else %}
    {{ return(true) }}
  {% endif %}
{% endmacro %}