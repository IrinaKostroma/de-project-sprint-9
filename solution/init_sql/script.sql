---
--- CDM
---
create table cdm.user_product_counters (
	id serial NOT NULL PRIMARY KEY
	, user_id uuid not null
	, product_id uuid not null
	, product_name varchar not null
	, order_cnt  integer DEFAULT 0 NOT NULL CHECK (order_cnt >= 0)
);
CREATE UNIQUE INDEX user_product_counters_idx ON cdm.user_product_counters (user_id, product_id);
-- drop index user_product_counters_idx;
-- drop table cdm.user_product_counters;

create table cdm.user_category_counters (
	id serial NOT NULL PRIMARY KEY
	, user_id uuid not null
	, category_id uuid not null
	, category_name varchar not null
	, order_cnt  integer DEFAULT 0 NOT NULL CHECK (order_cnt >= 0)
);
CREATE UNIQUE INDEX user_category_counters_idx ON cdm.user_category_counters (user_id, category_id);
-- drop index user_category_counters_idx;
-- drop table cdm.user_category_counters;


---
--- STG
---
create table stg.order_events (
	id serial NOT NULL PRIMARY KEY
	, object_id integer unique not null
	, payload json not null
	, object_type varchar not null
	, sent_dttm timestamp not null
);
-- drop table stg.order_events;

---
--- DDS
---
create table dds.h_user (
	h_user_pk uuid not null primary key
	, user_id varchar not null
	, load_dt timestamp without time zone not null
	, load_src varchar not null
);
-- drop table dds.h_user;

create table dds.h_product (
	h_product_pk uuid not null primary key
	, product_id varchar not null
	, load_dt timestamp without time zone not null
	, load_src varchar not null
);
drop table dds.h_product;

create table dds.h_category (
	h_category_pk uuid not null primary key
	, category_name varchar not null
	, load_dt timestamp without time zone not null
	, load_src varchar not null
);
-- drop table dds.h_category;

create table dds.h_restaurant (
	h_restaurant_pk uuid not null primary key
	, restaurant_id varchar not null
	, load_dt timestamp without time zone not null
	, load_src varchar not null
);
-- drop table dds.h_restaurant;

create table dds.h_order (
	h_order_pk uuid not null primary key
	, order_id integer not null
	, order_dt timestamp without time zone not null
	, load_dt timestamp without time zone not null
	, load_src varchar not null
);
-- drop table dds.h_order;

create table dds.l_order_product (
	hk_order_product_pk uuid not null primary key
	, h_order_pk uuid not null CONSTRAINT fk_l_order_product_order REFERENCES dds.h_order (h_order_pk)
	, h_product_pk uuid not null CONSTRAINT fk_l_order_product_product REFERENCES dds.h_product (h_product_pk)
	, load_dt timestamp without time zone not null
	, load_src varchar not null
);

create table dds.l_product_restaurant (
	hk_product_restaurant_pk uuid not null primary key
	, h_product_pk uuid not null CONSTRAINT fkl_product_restaurant_product REFERENCES dds.h_product (h_product_pk)
	, h_restaurant_pk uuid not null CONSTRAINT fk_l_product_restaurant_restaurant REFERENCES dds.h_restaurant (h_restaurant_pk)
	, load_dt timestamp without time zone not null
	, load_src varchar not null
);

create table dds.l_product_category (
	hk_product_category_pk uuid not null primary key
	, h_product_pk uuid not null CONSTRAINT fk_l_product_category_product REFERENCES dds.h_product (h_product_pk)
	, h_category_pk uuid not null CONSTRAINT fk_l_product_category_category REFERENCES dds.h_category (h_category_pk)
	, load_dt timestamp without time zone not null
	, load_src varchar not null
);

create table dds.l_order_user (
	hk_order_user_pk uuid not null primary key
	, h_order_pk uuid not null CONSTRAINT fk_l_order_user_order REFERENCES dds.h_order (h_order_pk)
	, h_user_pk uuid not null CONSTRAINT fk_l_order_user_user REFERENCES dds.h_user (h_user_pk)
	, load_dt timestamp without time zone not null
	, load_src varchar not null
);

create table dds.s_user_names (
	hk_user_names_hashdiff uuid not null primary key
	, h_user_pk uuid not null CONSTRAINT fk_s_user_names_user REFERENCES dds.h_user (h_user_pk)
	, username varchar not null
	, userlogin varchar not null
	, load_dt timestamp without time zone not null
	, load_src varchar not null
);
-- drop table dds.s_user_names

create table dds.s_product_names (
	hk_product_names_hashdiff uuid not null primary key
	, h_product_pk uuid not null CONSTRAINT fk_s_product_names REFERENCES dds.h_product (h_product_pk)
	, name varchar not null
	, load_dt timestamp without time zone not null
	, load_src varchar not null
);

create table dds.s_restaurant_names (
	hk_restaurant_names_hashdiff uuid not null primary key
	, h_restaurant_pk uuid not null CONSTRAINT fk_s_restaurant_names REFERENCES dds.h_restaurant (h_restaurant_pk)
	, name varchar not null
	, load_dt timestamp without time zone not null
	, load_src varchar not null
);
-- drop table dds.s_restaurant_names

create table dds.s_order_cost (
	hk_order_cost_hashdiff uuid not null primary key
	, h_order_pk uuid not null CONSTRAINT fk_s_order_cost_order REFERENCES dds.h_order (h_order_pk)
	, cost decimal(19, 5) not null
	, payment decimal(19, 5) not null
	, load_dt timestamp without time zone not null
	, load_src varchar not null
);

create table dds.s_order_status (
	hk_order_status_hashdiff uuid not null primary key
	, h_order_pk uuid not null CONSTRAINT fk_s_order_status_order REFERENCES dds.h_order (h_order_pk)
	, status varchar not null
	, load_dt timestamp without time zone not null
	, load_src varchar not null
);
