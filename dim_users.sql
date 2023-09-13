/*
file: columns_dim_users.sql
description: amends the column types and primary key
*/

do $$
declare
 	max_length integer;
	row_exists integer = 0;
begin 
 	-- first_name
	alter table dim_users alter column first_name set data type varchar(255);
 	-- last_name
	alter table dim_users alter column last_name set data type varchar(255);
	-- date_of_birth
	alter table dim_users alter column date_of_birth set data type date USING date_of_birth::date;
	-- country_code
	select max(length(cast(country_code as text))) into max_length from dim_users;
	execute 'alter table dim_users alter column country_code set data type varchar(' || max_length || ')';	
	-- user_uuid
	alter table dim_users alter column user_uuid set data type uuid USING user_uuid::uuid;
	-- join_date
	alter table dim_users alter column join_date set data type date USING join_date::date;
	-- primary key
	select 1 into row_exists
	from 	INFORMATION_SCHEMA.TABLE_CONSTRAINTS 
	where 	CONSTRAINT_TYPE = 'PRIMARY KEY' and 
			TABLE_NAME = 'dim_users'  
	limit 1;
	if row_exists = 1 then
		ALTER TABLE dim_users DROP CONSTRAINT dim_users_details_pkey CASCADE;
	end if;	
	alter table dim_users add primary key (user_uuid);
end; $$ ; 