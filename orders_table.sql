/*
file: orders_table.sql
description: amends the column types
*/

DO $$
DECLARE
 	max_length integer;
BEGIN 
 	-- date_uuid
	ALTER TABLE orders_table ALTER COLUMN date_uuid SET DATA TYPE uuid USING date_uuid::UUID;
 	-- date_uuid
	ALTER TABLE orders_table ALTER COLUMN user_uuid SET DATA TYPE uuid USING user_uuid::UUID;
	-- card_number
	SELECT max(LENGTH(CAST(card_number AS TEXT))) INTO max_length FROM orders_table;
	EXECUTE 'ALTER TABLE orders_table ALTER COLUMN card_number SET DATA TYPE VARCHAR(' || max_length || ')';
	-- store_code
	SELECT max(LENGTH(CAST(store_code AS TEXT))) INTO max_length FROM orders_table;
	EXECUTE 'ALTER TABLE orders_table ALTER COLUMN store_code SET DATA TYPE VARCHAR(' || max_length || ')';	
	-- product_code
	SELECT max(LENGTH(CAST(product_code AS TEXT))) INTO max_length FROM orders_table;
	EXECUTE 'ALTER TABLE orders_table ALTER COLUMN product_code SET DATA TYPE VARCHAR(' || max_length || ')';
	-- product_quantity
	ALTER TABLE orders_table ALTER COLUMN product_quantity SET DATA TYPE SMALLINT;
    -- remove records in the fact table that are NOT IN the dimension tables
	-- dim_card_details
	-- DELETE FROM orders_table ord
	-- WHERE card_number NOT IN 
	-- (SELECT card_number FROM dim_card_details);
	-- -- dim_date_times
	-- DELETE FROM orders_table ord
	-- WHERE date_uuid NOT IN 
	-- (SELECT date_uuid FROM dim_date_times);
	-- -- dim_store_details
	-- DELETE FROM orders_table ord
	-- WHERE store_code NOT IN 
	-- (SELECT store_code FROM dim_store_details);
	-- -- dim_users
	-- DELETE FROM orders_table ord
	-- WHERE user_uuid NOT IN 
	-- (SELECT user_uuid FROM dim_users);
	-- dim_products
--	DELETE FROM orders_table ord
--	WHERE product_code NOT IN 
--	(SELECT product_code FROM dim_products);
	commit;
	-- foreign keys
	ALTER TABLE orders_table
    ADD CONSTRAINT fk_card_number FOREIGN KEY (card_number) REFERENCES dim_card_details(card_number) ON UPDATE CASCADE;
	ALTER TABLE orders_table
    ADD CONSTRAINT fk_date_uuid FOREIGN KEY (date_uuid) REFERENCES dim_date_times(date_uuid) ON UPDATE CASCADE;
	ALTER TABLE orders_table
    ADD CONSTRAINT fk_store_code FOREIGN KEY (store_code) REFERENCES dim_store_details(store_code) ON UPDATE CASCADE;
	ALTER TABLE orders_table
    ADD CONSTRAINT fk_user_uuid FOREIGN KEY (user_uuid) REFERENCES dim_users(user_uuid) ON UPDATE CASCADE;
--  ALTER TABLE orders_table
--  ADD CONSTRAINT fk_product_code FOREIGN KEY (product_code) REFERENCES dim_products(product_code);

end; $$ ; 