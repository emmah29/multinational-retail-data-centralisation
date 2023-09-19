/*
file: dim_users.sql
description: amends the column types and primary key
*/

DO $$
DECLARE
 	max_length INTEGER;
	row_exists INTEGER = 0;
BEGIN 
 	-- first_name
	ALTER TABLE dim_users ALTER COLUMN first_name SET DATA TYPE VARCHAR(255);
 	-- last_name
	ALTER TABLE dim_users ALTER COLUMN last_name SET DATA TYPE VARCHAR(255);
	-- date_of_birth
	ALTER TABLE dim_users ALTER COLUMN date_of_birth SET DATA TYPE DATE USING date_of_birth::DATE;
	-- country_code
	SELECT max(length(cast(country_code as text))) INTO max_length FROM dim_users;
	EXECUTE 'ALTER TABLE dim_users ALTER COLUMN country_code SET DATA TYPE VARCHAR(' || max_length || ')';	
	-- user_uuid
	ALTER TABLE dim_users ALTER COLUMN user_uuid SET DATA TYPE UUID USING user_uuid::UUID;
	-- join_date
	ALTER TABLE dim_users ALTER COLUMN join_date SET DATA TYPE date USING join_date::date;
	-- primary key
	SELECT 1 INTO row_exists
	FROM 	INFORMATION_SCHEMA.TABLE_CONSTRAINTS 
	WHERE 	CONSTRAINT_TYPE = 'PRIMARY KEY' AND 
			TABLE_NAME = 'dim_users'  
	LIMIT 1;
	IF row_exists = 1 THEN
		ALTER TABLE dim_users DROP CONSTRAINT dim_users_details_pkey CASCADE;
	END IF;	
	ALTER TABLE dim_users ADD PRIMARY KEY (user_uuid);
end; $$ ; 