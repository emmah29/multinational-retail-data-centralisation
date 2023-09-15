
/*
file: columns_dim_date_times.sql
description: amends the column types and adds primary key
*/

DO $$
DECLARE
 	max_length INTEGER;
	row_exists INTEGER = 0;
BEGIN 
 	-- card_number
	SELECT max(LENGTH(CAST(card_number AS TEXT))) INTO max_length FROM dim_card_details;
	EXECUTE 'alter table dim_card_details alter column card_number set data type varchar(' || max_length || ')';	
 	-- expiry_date
	SELECT max(LENGTH(CAST(expiry_date AS TEXT))) INTO max_length FROM dim_card_details;
	EXECUTE 'alter table dim_card_details alter column expiry_date set data type varchar(' || max_length || ')';		
	-- date_payment_confirmed
	ALTER TABLE dim_card_details alter column date_payment_confirmed SET DATA TYPE DATE USING date_payment_confirmed::DATE;
	-- primary key
	SELECT 1 INTO row_exists
	FROM 	INFORMATION_SCHEMA.TABLE_CONSTRAINTS 
	WHERE 	CONSTRAINT_TYPE = 'PRIMARY KEY' AND 
			TABLE_NAME = 'dim_card_details' 
	LIMIT 1;
	IF row_exists = 1 THEN
		ALTER TABLE dim_card_details DROP CONSTRAINT dim_card_details_pkey CASCADE;
	END IF;
	ALTER TABLE dim_card_details add PRIMARY KEY (card_number);

END; $$ ; 