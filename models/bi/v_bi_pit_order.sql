{{
    config(
        enabled=True
    )
}}

{%- set yaml_metadata -%}
source_model: 'hub_customer'
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{% set source_model = metadata_dict['source_model'] %}

{% set derived_columns = metadata_dict['derived_columns'] %}

{% set hashed_columns = metadata_dict['hashed_columns'] %}

{% set dPIT = "'02-19-2022 23:59:59.999 +0500'" %}

WITH bi AS (
{{ dbtvault.stage(include_source_columns=true,
                  source_model=source_model,
                  derived_columns=derived_columns,
                  hashed_columns=hashed_columns,
                  ranked_columns=none) }}
)

select hc.customer_key , s1.first_name , s1.last_name , s1.email, s2.country , s2.age, s1.effective_from as s1_eff_from, s2.effective_from as s2_eff_from 
from hub_customer hc 
left join (select * from sat_customer_details scd   
				where scd.effective_from in 
					(select max(scd2.effective_from) 
					from sat_customer_details scd2 
					where scd.customer_pk = scd2.customer_pk 
					and scd2.effective_from <= {{dPIT}})) s1
	on hc.customer_pk = s1.customer_pk
left join (select * from sat_customer_crm_details sccd
				where sccd.effective_from in 
					(select max(sccd2.effective_from) 
					from sat_customer_crm_details sccd2 
					where sccd.customer_pk = sccd2.customer_pk 
					and sccd2.effective_from <= {{dPIT}})) s2
	on hc.customer_pk = s2.customer_pk
order by hc.customer_key
