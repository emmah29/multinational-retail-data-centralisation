/*
file: orders_table.sql
description: amends the column types
*/

do $$
declare
 	max_length integer;
begin 
 	-- date_uuid
	alter table orders_table alter column date_uuid set data type uuid USING date_uuid::uuid;
 	-- date_uuid
	alter table orders_table alter column user_uuid set data type uuid USING user_uuid::uuid;
	-- card_number
	select max(length(cast(card_number as text))) into max_length from orders_table;
	execute 'alter table orders_table alter column card_number set data type varchar(' || max_length || ')';
	-- store_code
	select max(length(cast(store_code as text))) into max_length from orders_table;
	execute 'alter table orders_table alter column store_code set data type varchar(' || max_length || ')';	
	-- product_code
	select max(length(cast(product_code as text))) into max_length from orders_table;
	execute 'alter table orders_table alter column product_code set data type varchar(' || max_length || ')';
	-- product_quantity
	alter table orders_table alter column product_quantity set data type smallint;
    -- remove records in the fact table that are not in the dimension tables
	-- dim_card_details
	delete from orders_table ord
	where card_number not in 
	(select card_number from dim_card_details);
	-- dim_date_times
	delete from orders_table ord
	where date_uuid not in 
	(select date_uuid from dim_date_times);
	-- dim_store_details
	delete from orders_table ord
	where store_code not in 
	(select store_code from dim_store_details);
	-- dim_users
	delete from orders_table ord
	where user_uuid not in 
	(select user_uuid from dim_users);
	-- dim_products
--	delete from orders_table ord
--	where product_code not in 
--	(select product_code from dim_products);
	commit;
	-- foreign keys
	ALTER TABLE orders_table
    ADD CONSTRAINT fk_card_number FOREIGN KEY (card_number) REFERENCES dim_card_details(card_number);
	ALTER TABLE orders_table
    ADD CONSTRAINT fk_date_uuid FOREIGN KEY (date_uuid) REFERENCES dim_date_times(date_uuid);
	ALTER TABLE orders_table
    ADD CONSTRAINT fk_store_code FOREIGN KEY (store_code) REFERENCES dim_store_details(store_code);
	ALTER TABLE orders_table
    ADD CONSTRAINT fk_user_uuid FOREIGN KEY (user_uuid) REFERENCES dim_users(user_uuid);
--  ALTER TABLE orders_table
--  ADD CONSTRAINT fk_product_code FOREIGN KEY (product_code) REFERENCES dim_products(product_code);

end; $$ ; 