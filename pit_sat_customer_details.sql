/*�������� ��������� ����������� ������� ��������
��������� ������������� Point-in-time table ������� ������� ���������� �������� ������� (first name, last name, email, country, age) �� �������� ������ �������.
��������� ���������� ������������� � ������ �������, ��� ������� ���� ������������ ���������, �������� ��� ������� ���������� ������.
*/
--$dPIT='02-19-2022 23:59:59.999 +0500';
select hc.customer_key , s1.first_name , s1.last_name , s1.email, s2.country , s2.age, s1.effective_from , s2.effective_from  
from hub_customer hc 
left join (select * from sat_customer_details scd   
				where scd.effective_from in 
					(select max(scd2.effective_from) 
					from sat_customer_details scd2 
					where scd.customer_pk = scd2.customer_pk 
					and scd2.effective_from <= $dPIT)) s1
	on hc.customer_pk = s1.customer_pk
left join (select * from sat_customer_crm_details sccd
				where sccd.effective_from in 
					(select max(sccd2.effective_from) 
					from sat_customer_crm_details sccd2 
					where sccd.customer_pk = sccd2.customer_pk 
					and sccd2.effective_from <= $dPIT)) s2
	on hc.customer_pk = s2.customer_pk
order by hc.customer_key;

/*��������� ������� ������ ��� Data Vault
�������� ��������� ���������� ������� � ������� ����������� ������ � ������� ������
�������� ��� ������� � ������ dbt, ����������������� � ��������������� � table, view
*/
select date_part('year', sod.order_date) as year, date_part('week',sod.order_date) week , sod.status, count(sod.order_pk) as kol_orders
from hub_order ho left join sat_order_details sod on (ho.order_pk=sod.order_pk)
group by sod.status, date_part('week',sod.order_date), date_part('year',sod.order_date)
order by year, week, status;
