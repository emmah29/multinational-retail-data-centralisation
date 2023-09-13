do $$
declare
	table_existence boolean;
begin
	
	select exists ( select 1 from pg_tables where tablename = 'dim_date_times' ) into table_existence;
	if table_existence then 
		drop table dim_date_times cascade;
	end if;
	
	select exists ( select 1 from pg_tables where tablename = 'dim_card_details' ) into table_existence;
	if table_existence then 
		drop table dim_card_details cascade;
	end if;
	
	select exists ( select 1 from pg_tables where tablename = 'dim_store_details' ) into table_existence;
	if table_existence then 
		drop table dim_store_details cascade;
	end if;
	
	select exists ( select 1 from pg_tables where tablename = 'dim_users' ) into table_existence;
	if table_existence then 
		drop table dim_users cascade;
	end if;
	
	select exists ( select 1 from pg_tables where tablename = 'orders_table' ) into table_existence;
	if table_existence then 
		drop table orders_table cascade;
	end if;
	
end; $$ ; 
	