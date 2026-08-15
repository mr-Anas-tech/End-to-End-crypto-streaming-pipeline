-- Price Change Percentage Helper
{% macro calc_price_change_pct(open_price, close_price) %}
    ROUND(
        ( (CAST({{ close_price }} AS DOUBLE) - CAST({{ open_price }} AS DOUBLE)) / CAST({{ open_price }} AS DOUBLE) ) * 100,
        2
    )
{% endmacro %}

-- Price Rounding Helper
{% macro round_price(price_column, decimals=2) %}
    ROUND(CAST({{ price_column }} AS DOUBLE), {{ decimals }})
{% endmacro %}