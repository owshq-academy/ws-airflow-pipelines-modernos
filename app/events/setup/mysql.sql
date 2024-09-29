drop table owshq.commerce;
drop table owshq.computer;
drop table owshq.device;

create table owshq.commerce
(
	incr int AUTO_INCREMENT,
    id bigint null,
	uid text null,
	color text null,
	department text null,
	material text null,
	product_name text null,
	price double null,
	price_string text null,
	promo_code text null,
	user_id bigint null,
	dt_current_timestamp datetime null,
    PRIMARY KEY (incr)
);

create table owshq.computer
(
	incr int AUTO_INCREMENT,
    id bigint null,
	uid text null,
	platform text null,
	type text null,
	os text null,
	stack text null,
	user_id bigint null,
	dt_current_timestamp datetime null,
	PRIMARY KEY (incr)
);

create table owshq.device
(
	incr int AUTO_INCREMENT,
    id bigint null,
	uid text null,
	build_number bigint null,
	manufacturer text null,
	model text null,
	platform text null,
	serial_number text null,
	version bigint null,
	user_id bigint null,
	dt_current_timestamp datetime null,
	PRIMARY KEY (incr)
);