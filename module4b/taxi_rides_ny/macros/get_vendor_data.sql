
{% macro get_vendor_names(vendor_id_column) %}
case
    when {{ vendor_id_column }} = 1 then 'Creative Mobile Technologies, LLC'
    when {{ vendor_id_column }} = 2 then 'VeriFone Inc.'
    when {{ vendor_id_column }} = 4 then 'Unknown Vendor'
end
{% endmacro %}