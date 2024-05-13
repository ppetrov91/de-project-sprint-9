CREATE SCHEMA IF NOT EXISTS dds;

CREATE TABLE IF NOT EXISTS dds.h_order (
    h_order_pk UUID PRIMARY KEY,
    order_id INT NOT NULL UNIQUE,
    order_dt TIMESTAMP NOT NULL,
    load_dt TIMESTAMP NOT NULL,
    load_src VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS dds.h_user (
    h_user_pk UUID PRIMARY KEY,
    user_id VARCHAR NOT NULL UNIQUE,
    load_dt TIMESTAMP NOT NULL,
    load_src VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS dds.h_product (
    h_product_pk UUID PRIMARY KEY,
    product_id VARCHAR NOT NULL UNIQUE,
    load_dt  TIMESTAMP NOT NULL,
    load_src VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS dds.h_category (
    h_category_pk UUID PRIMARY KEY,
    category_name VARCHAR NOT NULL UNIQUE,
    load_dt TIMESTAMP NOT NULL,
    load_src VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS dds.h_restaurant (
    h_restaurant_pk UUID PRIMARY KEY,
    restaurant_id VARCHAR NOT NULL UNIQUE,
    load_dt TIMESTAMP NOT NULL,
    load_src VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS dds.l_order_product (
    hk_order_product_pk uuid not null primary key,
    h_order_pk uuid not null,
    h_product_pk uuid not null,
    load_dt TIMESTAMP NOT NULL,
    load_src VARCHAR NOT NULL,
    CONSTRAINT l_order_product_h_order_product_ukey UNIQUE(h_order_pk, h_product_pk),
    CONSTRAINT l_order_product_h_order_pk_fk FOREIGN KEY(h_order_pk) REFERENCES dds.h_order(h_order_pk),
    CONSTRAINT l_order_product_h_product_pk_fk FOREIGN KEY(h_product_pk) REFERENCES dds.h_product(h_product_pk)
);

CREATE TABLE IF NOT EXISTS dds.l_product_restaurant (
    hk_product_restaurant_pk uuid not null primary key,
    h_product_pk uuid not null,
    h_restaurant_pk uuid not null,
    load_dt TIMESTAMP NOT NULL,
    load_src VARCHAR NOT NULL,
    CONSTRAINT l_product_restaurant_h_product_restaurant_ukey UNIQUE(h_product_pk, h_restaurant_pk),
    CONSTRAINT l_product_restaurant_h_restaurant_pk_fk FOREIGN KEY(h_restaurant_pk) REFERENCES dds.h_restaurant(h_restaurant_pk),
    CONSTRAINT l_product_restaurant_h_product_pk_fk FOREIGN KEY(h_product_pk) REFERENCES dds.h_product(h_product_pk)
);

CREATE TABLE IF NOT EXISTS dds.l_product_category (
    hk_product_category_pk uuid not null primary key,
    h_product_pk uuid not null,
    h_category_pk uuid not null,
    load_dt TIMESTAMP NOT NULL,
    load_src VARCHAR NOT NULL,
    CONSTRAINT l_product_category_h_product_category_ukey UNIQUE(h_product_pk, h_category_pk),
    CONSTRAINT l_product_category_h_category_pk_fk FOREIGN KEY(h_category_pk) REFERENCES dds.h_category(h_category_pk),
    CONSTRAINT l_product_category_h_product_pk_fk FOREIGN KEY(h_product_pk) REFERENCES dds.h_product(h_product_pk)
);

CREATE TABLE IF NOT EXISTS dds.l_order_user (
    hk_order_user_pk uuid not null primary key,
    h_order_pk uuid not null,
    h_user_pk uuid not null,
    load_dt TIMESTAMP NOT NULL,
    load_src VARCHAR NOT NULL,
    CONSTRAINT l_order_user_horder_user_ukey UNIQUE(h_order_pk, h_user_pk),
    CONSTRAINT l_order_user_h_order_pk_fk FOREIGN KEY(h_order_pk) REFERENCES dds.h_order(h_order_pk),
    CONSTRAINT l_order_user_h_user_pk_fk FOREIGN KEY(h_user_pk) REFERENCES dds.h_user(h_user_pk)
);

CREATE TABLE IF NOT EXISTS dds.s_user_names (
    hk_user_names_pk uuid not null primary key,
    h_user_pk uuid not null,
    username varchar not null,
    userlogin varchar not null,
    load_src VARCHAR NOT NULL,
    start_dt TIMESTAMP NOT NULL,
    end_dt TIMESTAMP NOT NULL,
    CONSTRAINT s_user_names_h_user_pk_end_dt_ukey UNIQUE(h_user_pk, end_dt),
    CONSTRAINT s_user_names_h_user_pk_fk FOREIGN KEY(h_user_pk) REFERENCES dds.h_user(h_user_pk)
);

CREATE TABLE IF NOT EXISTS dds.s_product_names (
    hk_product_names_pk uuid not null primary key,
    h_product_pk uuid not null,
    productname varchar not null,
    start_dt TIMESTAMP NOT NULL,
    load_src VARCHAR NOT NULL,
    end_dt TIMESTAMP NOT NULL,
    CONSTRAINT s_product_names_h_product_pk_end_dt_ukey UNIQUE(h_product_pk, end_dt),
    CONSTRAINT s_product_names_h_product_pk_fk FOREIGN KEY(h_product_pk) REFERENCES dds.h_product(h_product_pk)
);

CREATE TABLE IF NOT EXISTS dds.s_restaurant_names (
    hk_restaurant_names_pk uuid not null primary key,
    h_restaurant_pk uuid not null,
    restaurantname varchar not null,
    load_src VARCHAR NOT NULL,
    start_dt TIMESTAMP NOT NULL,
    end_dt TIMESTAMP NOT NULL,
    CONSTRAINT s_restaurant_names_h_restaurant_pk_end_dt_ukey UNIQUE(h_restaurant_pk, end_dt),
    CONSTRAINT s_restaurant_names_h_restaurant_pk_fk FOREIGN KEY(h_restaurant_pk) REFERENCES dds.h_restaurant(h_restaurant_pk)
);

CREATE TABLE IF NOT EXISTS dds.s_order_cost (
    hk_order_cost_pk uuid not null primary key,
    h_order_pk uuid not null,
    cost decimal(19,5) not null,
    payment decimal(19,5) not null,
    load_src VARCHAR NOT NULL,
    start_dt TIMESTAMP NOT NULL,
    end_dt TIMESTAMP NOT NULL,
    CONSTRAINT s_order_cost_h_order_pk_end_dt_ukey UNIQUE(h_order_pk, end_dt),
    CONSTRAINT s_order_cost_h_order_pk_fk FOREIGN KEY(h_order_pk) REFERENCES dds.h_order(h_order_pk)
);

CREATE TABLE IF NOT EXISTS dds.s_order_status (
    hk_order_status_pk uuid not null primary key,
    h_order_pk uuid not null,
    status VARCHAR NOT NULL,
    load_src VARCHAR NOT NULL,
    start_dt TIMESTAMP NOT NULL,
    end_dt TIMESTAMP NOT NULL,
    CONSTRAINT s_order_status_h_order_pk_end_dt_ukey UNIQUE(h_order_pk, end_dt),
    CONSTRAINT s_order_status_h_order_pk_fk FOREIGN KEY(h_order_pk) REFERENCES dds.h_order(h_order_pk)
);
