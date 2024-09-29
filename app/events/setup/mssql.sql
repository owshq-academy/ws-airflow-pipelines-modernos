drop table dbo.bank;
drop table dbo.credit_card;
drop table dbo.subscription;
drop table dbo.company;

create table dbo.bank
(
    incr int IDENTITY(1,1) PRIMARY KEY,
	id bigint,
	uid varchar(max),
	account_number varchar(max),
	iban varchar(max),
	bank_name varchar(max),
	routing_number varchar(max),
	swift_bic varchar(max),
	user_id bigint,
	dt_current_timestamp datetime,
	dt_creation datetime2 default CURRENT_TIMESTAMP
)
go

create table dbo.credit_card
(
    incr int IDENTITY(1,1) PRIMARY KEY,
	id bigint,
	uid varchar(max),
	credit_card_number varchar(max),
	credit_card_expiry_date varchar(max),
	credit_card_type varchar(max),
	user_id bigint,
	dt_current_timestamp datetime,
	dt_creation datetime2 default CURRENT_TIMESTAMP
)
go

create table dbo.subscription
(
    incr int IDENTITY(1,1) PRIMARY KEY,
	id bigint,
	uid varchar(max),
	[plan] varchar(max),
	status varchar(max),
	payment_method varchar(max),
	subscription_term varchar(max),
	payment_term varchar(max),
	user_id bigint,
	dt_current_timestamp datetime,
	dt_creation datetime2 default CURRENT_TIMESTAMP
)
go

create table dbo.company
(
    incr int IDENTITY(1,1) PRIMARY KEY,
	id bigint,
	uid varchar(max),
	business_name varchar(max),
	suffix varchar(max),
	industry varchar(max),
	catch_phrase varchar(max),
	buzzword varchar(max),
	bs_company_statement varchar(max),
	employee_identification_number varchar(max),
	duns_number varchar(max),
	logo varchar(max),
	type varchar(max),
	phone_number varchar(max),
	full_address varchar(max),
	latitude float,
	longitude float,
	user_id bigint,
	dt_current_timestamp datetime,
	dt_creation datetime2 default CURRENT_TIMESTAMP
)
go