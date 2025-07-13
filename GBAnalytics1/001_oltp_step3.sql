--step3 create actual OLTP tables by using BIG temp tables as base from xxxx_step3
--

Use Resellers2ndHandStuffOLTP
SET NOCOUNT OFF


DROP TABLE IF EXISTS RESELLERS_2ND_HAND_STUFF_COUPONS
CREATE TABLE RESELLERS_2ND_HAND_STUFF_COUPONS
(
	id INT UNIQUE,
	created_at DATETIME,
	days_to_live INT,
	discount_percent INT,
	discount_price INT,
	minimum_order INT,
	name NVARCHAR(100),
	updated_at DATETIME,
	usage_limit INT,
	--todo: sql server doesnt support JSON
	--skipping for now user_phones INT
)


DROP TABLE IF EXISTS RESELLERS_2ND_HAND_STUFF_ITEMS
CREATE TABLE RESELLERS_2ND_HAND_STUFF_ITEMS
(
	id INT UNIQUE,
	created_at DATETIME,
	description NVARCHAR(100),
	--todo: sql server doesnt support array.. so skipping dietary_tags_list
	is_available BIT,
	item_exists BIT,
	section_id NVARCHAR(100),
	name NVARCHAR(100),
	--todo: skipping photos since not able to be easily auto generated
	price FLOAT,
	--todo: sql server doesnt support array..so skipping recommendations
	req_cust BIT,
	resellers_id INT,
	string_id NVARCHAR(50),
	updated_at DATETIME
)

DROP TABLE IF EXISTS RESELLERS_2ND_HAND_STUFF_ITEMSECTIONS
CREATE TABLE RESELLERS_2ND_HAND_STUFF_ITEMSECTIONS
(
	id INT UNIQUE,
	created_at DATETIME,
	is_available BIT,
	name NVARCHAR(100),
	resellers_id INT,
	updated_at DATETIME
)

DROP TABLE IF EXISTS RESELLERS_2ND_HAND_STUFF_ORDERS
CREATE TABLE RESELLERS_2ND_HAND_STUFF_ORDERS
(
	id INT UNIQUE,
	created_at DATETIME,
	--todo: for now skipping delivery stuff as that data and logic would probably complicate db schema
	--delivery_id INT,
	--delivery_price FLOAT,
	--delivery_status NVARCHAR(100), --todo:  consider creating enumerated strings to emulate limited set of real statuses..?
	discount_percent INT, --use 2 digit int < 20
	item_price FLOAT, --use 2 digit float < 20
	items_in_order_id INT,
	nonce INT, --very large INT
	payment_status BIT,
	price FLOAT, --use 2 digit float < 20
	service_fee FLOAT, --use 2 digit float < 1 perhaps..?
	status BIT,
	time_slot INT, --use INT < 20
	updated_at DATETIME,
	user_id INT,
	user_ordering_location_address1 NVARCHAR(100),
	user_ordering_location_city NVARCHAR(50),
	user_ordering_location_location NVARCHAR(50),
	user_ordering_location_place NVARCHAR(50),
	user_ordering_location_state NVARCHAR(50),
	user_ordering_location_zip INT
)

DROP TABLE IF EXISTS RESELLERS_2ND_HAND_STUFF_ITEMS_IN_ORDER
CREATE TABLE RESELLERS_2ND_HAND_STUFF_ITEMS_IN_ORDER
(
	id INT UNIQUE,
	order_id INT,  --note:  this ID is not updated until the order is created first, maybe remove this..?
	count TINYINT,
	pickup_status BIT,
	price FLOAT, --USE 2 digit float < 20
	item_id INT,
	resellers_id INT
)

DROP TABLE IF EXISTS RESELLERS_2ND_HAND_STUFF_RESELLERS
CREATE TABLE RESELLERS_2ND_HAND_STUFF_RESELLERS
(
	id INT UNIQUE,
	budget NVARCHAR(50), --use enum-like category BUDGET_CATG
	kind_of_business NVARCHAR(50),  --use enum like category KIND_OF_BUSINESS
	contact_address1 NVARCHAR(50), 
	contact_city NVARCHAR(50),
	contact_place NVARCHAR(50),
	contact_state NVARCHAR(50),
	contact_timezone NVARCHAR(50),
	contact_zip INT,
	created_at DATETIME,
	description NVARCHAR(50),
	--remove these ids since not required
	--door_dash_id INT,
	--door_dash_url NVARCHAR(100),
	store_type TINYINT,
	is_available BIT,
	is_covid_complaint BIT,
	is_open BIT,
	last_crawled DATETIME,
	name NVARCHAR(100),
	operating_hrs TINYINT,
	string_id NVARCHAR(50),
	time_slot_data TINYINT,
	--skipping since time_slot_ids no array support at moment
	updated_at DATETIME,
	views INT, -- med size INT < 100
	yelp_id NVARCHAR(100)
)

DROP TABLE IF EXISTS RESELLERS_2ND_HAND_STUFF_TAXRATES
CREATE TABLE RESELLERS_2ND_HAND_STUFF_TAXRATES
(
	id INT UNIQUE,
	city NVARCHAR(50),
	rates INT --2 digit INT percentage
)

DROP TABLE IF EXISTS RESELLERS_2ND_HAND_STUFF_TOKENS
CREATE TABLE RESELLERS_2ND_HAND_STUFF_TOKENS
(
	id INT UNIQUE,
	blacklisted BIT,
	created_at DATETIME,
	expires DATETIME,
	token NVARCHAR(100),
	updated_at DATETIME,
	user_name NVARCHAR(100),
	user_id INT,  --foreign key for user_id associated with this token
)

DROP TABLE IF EXISTS RESELLERS_2ND_HAND_STUFF_USERS
CREATE TABLE RESELLERS_2ND_HAND_STUFF_USERS
(
	id INT UNIQUE,
	active BIT,
	braintree_customer_id NVARCHAR(50),
	business TINYINT, -- <10 integer, enum
	business_role TINYINT, -- <10 integer, enum
	contact NVARCHAR(50),
	created_at DATETIME,
	cur_location_address NVARCHAR(50),
	cur_location_location NVARCHAR(50),
	dev NVARCHAR(50),
	email NVARCHAR(50),
	first_name NVARCHAR(50),
	hashed_password NVARCHAR(50),
	--image_filename skipping this so no auto generation of photo
	image_path NVARCHAR(50),
	is_active BIT,
	is_compliant BIT,
	last_name NVARCHAR(50),
	pass_code INT,  --LARGE INT > 100
	pass_code_expires DATETIME,
	phone_number NVARCHAR(50),
	reset_password_link NVARCHAR(256),
	role NVARCHAR(50),
	salt TINYINT,
	updated_at DATETIME,
	--used_coupons no support for arrays
)

