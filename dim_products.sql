/*
file: columns_dim_products.sql
description: amends the column types and primary key
*/

do $$
declare
 	max_length integer;
	row_exists integer = 0;
begin 
 	-- product_price
	alter table dim_products alter column product_price set data type float using product_price::float;
 	-- weight
	alter table dim_products alter column weight set data type float using weight::float;
	-- EAN
	select max(length(cast(EAN as text))) into max_length from dim_products;
	execute 'alter table dim_products alter column ean set data type varchar(' || max_length || ')';
	-- product_code
	select max(length(cast(product_code as text))) into max_length from orders_table;
	execute 'alter table dim_products alter column product_code set data type varchar(' || max_length || ')';	
	-- date_added
	alter table dim_products alter column date_added set data type date;
	-- uuid
	alter table dim_products alter column uuid set data type uuid;
	-- still_available
	--alter table dim_products alter column still_available set data type bool;
	-- weight_class
	select max(length(cast(weight_class as text))) into max_length from orders_table;
	execute 'alter table dim_products alter column weight_class set data type varchar(' || max_length || ')';	
	-- primary key
	select 1 into row_exists
	from 	INFORMATION_SCHEMA.TABLE_CONSTRAINTS 
	where 	CONSTRAINT_TYPE = 'PRIMARY KEY' and 
			TABLE_NAME = 'dim_products'  
	limit 1;
	if row_exists = 1 then
		ALTER TABLE dim_products DROP CONSTRAINT dim_products_pkey  CASCADE;
	end if;
	alter table dim_products add primary key (product_code);

end; $$ ; 
