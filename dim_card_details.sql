
/*
file: columns_dim_date_times.sql
description: amends the column types and adds primary key
*/

do $$
declare
 	max_length integer;
	row_exists integer = 0;
begin 
 	-- card_number
	select max(length(cast(card_number as text))) into max_length from dim_card_details;
	execute 'alter table dim_card_details alter column card_number set data type varchar(' || max_length || ')';	
 	-- expiry_date
	select max(length(cast(expiry_date as text))) into max_length from dim_card_details;
	execute 'alter table dim_card_details alter column expiry_date set data type varchar(' || max_length || ')';		
	-- date_payment_confirmed
	alter table dim_card_details alter column date_payment_confirmed set data type date using date_payment_confirmed::date;
	-- primary key
	select 1 into row_exists
	from 	INFORMATION_SCHEMA.TABLE_CONSTRAINTS 
	where 	CONSTRAINT_TYPE = 'PRIMARY KEY' and 
			TABLE_NAME = 'dim_card_details' 
	limit 1;
	if row_exists = 1 then
		ALTER TABLE dim_card_details DROP CONSTRAINT dim_card_details_pkey CASCADE;
	end if;
	alter table dim_card_details add primary key (card_number);

end; $$ ; 