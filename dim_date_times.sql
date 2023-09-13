
/*
file: columns_dim_date_times.sql
description: amends the column types and add primary key
*/

do $$
declare
 	max_length integer;
	row_exists integer = 0;
begin 
 	-- month
	select max(length(cast(month as text))) into max_length from dim_date_times;
	execute 'alter table dim_date_times alter column month set data type varchar(' || max_length || ')';	
 	-- year
	select max(length(cast(year as text))) into max_length from dim_date_times;
	execute 'alter table dim_date_times alter column year set data type varchar(' || max_length || ')';	
	-- day
	select max(length(cast(day as text))) into max_length from dim_date_times;
	execute 'alter table dim_date_times alter column day set data type varchar(' || max_length || ')';	
	-- time_period
	select max(length(cast(time_period as text))) into max_length from dim_date_times;
	execute 'alter table dim_date_times alter column time_period set data type varchar(' || max_length || ')';	
	-- date_uuid
	alter table dim_date_times alter column date_uuid set data type uuid using date_uuid::uuid;
	-- primary key
	select 1 into row_exists
	from 	INFORMATION_SCHEMA.TABLE_CONSTRAINTS 
	where 	CONSTRAINT_TYPE = 'PRIMARY KEY' and 
			TABLE_NAME = 'dim_date_times'  
	limit 1;
	if row_exists = 1 then
		ALTER TABLE dim_date_times DROP CONSTRAINT dim_date_times_pkey CASCADE;
	end if;
	alter table dim_date_times add primary key (date_uuid);
end; $$ ; 