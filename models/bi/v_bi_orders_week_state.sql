{{
    config(
        enabled=True
    )
}}

{%- set yaml_metadata -%}
source_model: 'sat_order_details'
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{% set source_model = metadata_dict['source_model'] %}

{% set derived_columns = metadata_dict['derived_columns'] %}

{% set hashed_columns = metadata_dict['hashed_columns'] %}


WITH bi AS (
{{ dbtvault.stage(include_source_columns=true,
                  source_model=source_model,
                  derived_columns=derived_columns,
                  hashed_columns=hashed_columns,
                  ranked_columns=none) }}
)
select  
	date_trunc('week',order_date) as cweek, 
	SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) as completed,
	SUM(CASE WHEN status='placed' THEN 1 ELSE 0 END) as placed,
	SUM(CASE WHEN status='returned' THEN 1 ELSE 0 END) as returned,
	SUM(CASE WHEN status='return_pending' THEN 1 ELSE 0 END) as return_pending,
	SUM(CASE WHEN status='shipped' THEN 1 ELSE 0 END) as shipped
from 
	sat_order_details sod 
group by 
	date_trunc('week',order_date)
order by 
	cweek

/*
select * from crosstab(
$$select  
	sod.status,
	date_part('week',sod.order_date) as cweek, 
	count(sod.order_pk) as kol_orders
from 
	sat_order_details sod 
group by 
	sod.status,
	date_part('week',sod.order_date)
order by 
	status,
	cweek
	$$
	,
    $$
        SELECT generate_series(1, 15)
    $$
) as ct(status text, "1" numeric, "2" numeric, "3" numeric, "4" numeric, "5" numeric, "6" numeric, "7" numeric, "8" numeric, "9" numeric, "10" numeric, "11" numeric, "12" numeric,"13" numeric, "14" numeric, "15" numeric)
*/