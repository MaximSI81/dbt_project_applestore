## Создаем к БД apple_db

#### сначала создаем БД apple_dwh и открываем sql редактор в этой БД и выполняем дальнейший скрипт
```sql
select * from pg_catalog.pg_available_extensions;

create extension postgres_fdw;

drop server if exists data_db_pg cascade;
create server data_db_pg foreign data wrapper postgres_fdw options (
	host 'localhost',
	dbname 'data_db',
	port '5432'
);

create user mapping for postgres server data_db_pg options (
	user 'postgres',
	password 'postgres'
);

drop schema if exists data_db_src;
create schema data_db_src authorization postgres;
import foreign schema public from server data_db_pg into data_db_src;
```