-- Macro for calculating anomaly scores using Z-score method
{% macro calculate_anomaly_score(value_column, mean_column, stddev_column) %}
    case 
        when {{ stddev_column }} > 0 then
            abs({{ value_column }} - {{ mean_column }}) / {{ stddev_column }}
        else 0
    end
{% endmacro %}

-- Macro for determining if a value is an anomaly
{% macro is_anomaly(z_score_column, threshold=3) %}
    case 
        when {{ z_score_column }} > {{ threshold }} then true
        else false
    end
{% endmacro %}

-- Macro for data quality scoring
{% macro calculate_data_quality_score(required_fields, optional_fields) %}
    (
        {% set total_weight = required_fields|length * 0.7 + optional_fields|length * 0.3 %}
        {% set required_weight = 0.7 / required_fields|length %}
        {% set optional_weight = 0.3 / optional_fields|length %}
        
        {% for field in required_fields %}
            case when {{ field }} is not null then {{ required_weight }} else 0 end
            {% if not loop.last %} + {% endif %}
        {% endfor %}
        
        {% if optional_fields|length > 0 %}
            +
            {% for field in optional_fields %}
                case when {{ field }} is not null then {{ optional_weight }} else 0 end
                {% if not loop.last %} + {% endif %}
            {% endfor %}
        {% endif %}
    )
{% endmacro %}