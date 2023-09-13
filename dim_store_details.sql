/*
file: columns_dim_store_details.sql
description: amENDs the column types AND primary key
*/

DO $$
DECLARE
 	max_length INTEGER;
	row_exists INTEGER = 0;
BEGIN 
 	-- longitude
	ALTER TABLE dim_store_details ALTER COLUMN longitude SET DATA TYPE FLOAT USING longitude::DOUBLE PRECISION;
 	-- locality
	ALTER TABLE dim_store_details ALTER COLUMN locality SET DATA TYPE VARCHAR(255);
	-- store_code
	SELECT max(length(cast(store_code as text))) into max_length FROM dim_store_details;
	EXECUTE 'ALTER TABLE dim_store_details ALTER COLUMN store_code SET DATA TYPE VARCHAR(' || max_length || ')';	
	-- staff_numbers
	ALTER TABLE dim_store_details ALTER COLUMN staff_numbers SET DATA TYPE SMALLINT USING staff_numbers::SMALLINT;
	-- opening_DATE
	ALTER TABLE dim_store_details ALTER COLUMN opening_date SET DATA TYPE DATE USING opening_DATE::DATE;
	-- store_type
	ALTER TABLE dim_store_details ALTER COLUMN store_type SET DATA TYPE VARCHAR(255);
    ALTER TABLE dim_store_details ALTER COLUMN store_type DROP NOT NULL;
	-- latitude
	ALTER TABLE dim_store_details ALTER COLUMN latitude SET DATA TYPE FLOAT USING latitude::DOUBLE PRECISION;
	-- country_code
	SELECT MAX(LENGTH(CAST(country_code AS TEXT))) INTO max_length FROM dim_store_details;
	EXECUTE 'ALTER TABLE dim_store_details ALTER COLUMN country_code SET DATA TYPE VARCHAR(' || max_length || ')';	
	-- continent
	ALTER TABLE dim_store_details ALTER COLUMN continent SET DATA TYPE VARCHAR(255);
	-- primary key
	SELECT 1 into row_exists
	FROM 	INFORMATION_SCHEMA.TABLE_CONSTRAINTS 
	WHERE 	CONSTRAINT_TYPE = 'PRIMARY KEY' AND 
			TABLE_NAME = 'dim_store_details'  
	LIMIT 1;
	IF row_exists = 1 THEN
		ALTER TABLE dim_store_details DROP CONSTRAINT dim_store_details_pkey CASCADE;
	END IF;
	ALTER TABLE dim_store_details add primary key (store_code);
END; $$ ; 
