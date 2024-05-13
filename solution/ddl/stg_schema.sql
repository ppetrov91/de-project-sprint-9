CREATE SCHEMA IF NOT EXISTS stg;

CREATE TABLE IF NOT EXISTS stg.order_events (
	id serial primary key not null,
	object_id int not null UNIQUE,
	payload jsonb not null,
	object_type varchar not null,
	sent_dttm timestamp not null
);
