/*
file: columns_dim_store_details.sql
description: amends the column types and primary key
*/

do $$
declare
 	max_length integer;
	row_exists integer = 0;
begin 
 	-- longitude
	alter table dim_store_details alter column longitude set data type FLOAT USING longitude::double precision;
 	-- locality
	alter table dim_store_details alter column locality set data type varchar(255);
	-- store_code
	select max(length(cast(store_code as text))) into max_length from dim_store_details;
	execute 'alter table dim_store_details alter column store_code set data type varchar(' || max_length || ')';	
	-- staff_numbers
	alter table dim_store_details alter column staff_numbers set data type smallint USING staff_numbers::smallint;
	-- opening_date
	alter table dim_store_details alter column opening_date set data type date USING opening_date::date;
	-- store_type
	alter table dim_store_details alter column store_type set data type varchar(255);
    alter table dim_store_details alter column store_type drop not null;
	-- latitude
	alter table dim_store_details alter column latitude set data type float USING latitude::double precision;
	-- country_code
	select max(length(cast(country_code as text))) into max_length from dim_store_details;
	execute 'alter table dim_store_details alter column country_code set data type varchar(' || max_length || ')';	
	-- continent
	alter table dim_store_details alter column continent set data type varchar(255);
	-- primary key
	select 1 into row_exists
	from 	INFORMATION_SCHEMA.TABLE_CONSTRAINTS 
	where 	CONSTRAINT_TYPE = 'PRIMARY KEY' and 
			TABLE_NAME = 'dim_store_details'  
	limit 1;
	if row_exists = 1 then
		ALTER TABLE dim_store_details DROP CONSTRAINT dim_store_details_pkey CASCADE;
	end if;
	alter table dim_store_details add primary key (store_code);
end; $$ ; 
