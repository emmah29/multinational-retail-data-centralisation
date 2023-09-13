DO $$
DECLARE
	table_existence BOOLEAN;
BEGIN
	
	SELECT EXISTS ( SELECT 1 FROM pg_tables WHERE tablename = 'dim_date_times' ) INTO table_existence;
	IF table_existence THEN 
		DROP TABLE dim_date_times CASCADE;
	END IF;
	
	SELECT EXISTS ( SELECT 1 FROM pg_tables WHERE tablename = 'dim_card_details' ) INTO table_existence;
	IF table_existence THEN 
		DROP TABLE dim_card_details CASCADE;
	END IF;
	
	SELECT EXISTS ( SELECT 1 FROM pg_tables WHERE tablename = 'dim_store_details' ) INTO table_existence;
	IF table_existence THEN 
		DROP TABLE dim_store_details CASCADE;
	END IF;
	
	SELECT EXISTS ( SELECT 1 FROM pg_tables WHERE tablename = 'dim_users' ) INTO table_existence;
	IF table_existence THEN 
		DROP TABLE dim_users CASCADE;
	END IF;
	
	SELECT EXISTS ( SELECT 1 FROM pg_tables WHERE tablename = 'orders_table' ) INTO table_existence;
	IF table_existence THEN 
		DROP TABLE orders_table CASCADE;
	END IF;
	
END; $$ ; 
	