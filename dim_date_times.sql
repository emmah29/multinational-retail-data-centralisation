
/*
file: dim_date_times.sql
description: amends the column types and add primary key
*/

DO $$
declare
 	max_length INTEGER;
	row_exists INTEGER = 0;
begin 
 	-- month
	SELECT max(LENGTH(CAST(month AS TEXT))) INTO max_length FROM dim_date_times;
	EXECUTE 'ALTER TABLE dim_date_times alter column month SET DATA TYPE varchar(' || max_length || ')';	
 	-- year
	SELECT max(LENGTH(CAST(year AS TEXT))) INTO max_length FROM dim_date_times;
	EXECUTE 'ALTER TABLE dim_date_times alter column year SET DATA TYPE varchar(' || max_length || ')';	
	-- day
	SELECT max(LENGTH(CAST(day AS TEXT))) INTO max_length FROM dim_date_times;
	EXECUTE 'ALTER TABLE dim_date_times alter column day SET DATA TYPE varchar(' || max_length || ')';	
	-- time_period
	SELECT max(LENGTH(CAST(time_period AS TEXT))) INTO max_length FROM dim_date_times;
	EXECUTE 'ALTER TABLE dim_date_times alter column time_period SET DATA TYPE varchar(' || max_length || ')';	
	-- date_uuid
	ALTER TABLE dim_date_times alter column date_uuid SET DATA TYPE uuid USING date_uuid::uuid;
	-- primary key
	SELECT 1 INTO row_exists
	FROM 	INFORMATION_SCHEMA.TABLE_CONSTRAINTS 
	WHERE 	CONSTRAINT_TYPE = 'PRIMARY KEY' AND 
			TABLE_NAME = 'dim_date_times'  
	LIMIT 1;
	IF row_exists = 1 THEN
		ALTER TABLE dim_date_times DROP CONSTRAINT dim_date_times_pkey CASCADE;
	end if;
	ALTER TABLE dim_date_times ADD PRIMARY KEY (date_uuid);
END; $$ ; 