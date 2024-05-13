CREATE SCHEMA IF NOT EXISTS cdm;

CREATE TABLE IF NOT EXISTS cdm.user_product_counters (
	id serial not null primary key,
	user_id uuid not null,
	product_id uuid not null,
	product_name varchar not null,
	order_cnt int not null CHECK(order_cnt >= 0),
	CONSTRAINT user_product_counters_user_product_id_ukey UNIQUE(user_id, product_id)
);

CREATE TABLE IF NOT EXISTS cdm.user_category_counters (
	id serial not null primary key,
	user_id uuid not null,
	category_id uuid not null,
	category_name varchar not null,
	order_cnt int not null CHECK(order_cnt >= 0),
	CONSTRAINT user_category_counters_user_category_id_ukey UNIQUE(user_id, category_id)
);
