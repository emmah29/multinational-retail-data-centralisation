/*
file: columns_dim_products.sql
description: amends the column types and primary key
*/

DO $$
DECLARE
 	max_length INTEGER;
	row_exists INTEGER = 0;
BEGIN 
 	-- product_price
	ALTER TABLE dim_products ALTER COLUMN product_price SET DATA TYPE FLOAT USING product_price::FLOAT;
 	-- weight
	ALTER TABLE dim_products ALTER COLUMN weight SET DATA TYPE FLOAT USING weight::FLOAT;
	-- EAN
	SELECT max(length(cast(EAN as text))) INTO max_length FROM dim_products;
	EXECUTE 'ALTER TABLE dim_products ALTER COLUMN ean SET DATA TYPE VARCHAR(' || max_length || ')';
	-- product_code
	SELECT max(length(cast(product_code as text))) INTO max_length FROM orders_table;
	EXECUTE 'ALTER TABLE dim_products ALTER COLUMN product_code SET DATA TYPE VARCHAR(' || max_length || ')';	
	-- date_added
	ALTER TABLE dim_products ALTER COLUMN date_added SET DATA TYPE DATE;
	-- uuid
	ALTER TABLE dim_products ALTER COLUMN uuid SET DATA TYPE UUID;
	-- still_available
	--ALTER TABLE dim_products ALTER COLUMN still_available SET DATA TYPE BOOL;
	-- weight_class
	SELECT max(length(cast(weight_class AS TEXT))) INTO max_length FROM orders_table;
	EXECUTE 'ALTER TABLE dim_products ALTER COLUMN weight_class SET DATA TYPE VARCHAR(' || max_length || ')';	
	-- primary key
	SELECT 1 INTO row_exists
	FROM 	INFORMATION_SCHEMA.TABLE_CONSTRAINTS 
	WHERE 	CONSTRAINT_TYPE = 'PRIMARY KEY' AND 
			TABLE_NAME = 'dim_products'  
	LIMIT 1;
	IF row_exists = 1 THEN
		ALTER TABLE dim_products DROP CONSTRAINT dim_products_pkey CASCADE;
	END IF;
	ALTER TABLE dim_products ADD PRIMARY KEY (product_code);

END; $$ ; 
