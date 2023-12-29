-- public.air_quality definition

-- Drop table

-- DROP TABLE public.air_quality;

CREATE TABLE public.air_quality (
	city_id serial4 NOT NULL,
	co float4 NULL,
	no2 float4 NULL,
	o3 float4 NULL,
	so2 float4 NULL,
	pm2_5 float4 NULL,
	pm10 float4 NULL,
	"name" varchar(50) NULL,
	last_updated varchar(50) NULL,
	CONSTRAINT air_quality_pkey PRIMARY KEY (city_id)
);


-- public."current" definition

-- Drop table

-- DROP TABLE public."current";

CREATE TABLE public."current" (
	city_id serial4 NOT NULL,
	last_updated timestamp NULL,
	wind_mph float4 NULL,
	wind_kph float4 NULL,
	wind_degree float4 NULL,
	wind_dir bpchar(4) NULL,
	temp_c float4 NULL,
	temp_f float4 NULL,
	is_day float4 NULL,
	"condition" varchar(100) NULL,
	pressure_mb float4 NULL,
	pressure_in float4 NULL,
	precip_mm float4 NULL,
	precip_in float4 NULL,
	humidity float4 NULL,
	cloud float4 NULL,
	feelslike_c float4 NULL,
	feelslike_f float4 NULL,
	vis_km float4 NULL,
	vis_miles float4 NULL,
	uv float4 NULL,
	gust_mph float4 NULL,
	gust_kph float4 NULL,
	"name" varchar(50) NULL,
	CONSTRAINT current_pkey PRIMARY KEY (city_id)
);


-- public."location" definition

-- Drop table

-- DROP TABLE public."location";

CREATE TABLE public."location" (
	city_id serial4 NOT NULL,
	"name" varchar(100) NOT NULL,
	region varchar(100) NOT NULL,
	country varchar(100) NULL,
	tz_id varchar(100) NOT NULL,
	lat float4 NULL,
	lon float4 NULL,
	iso_code varchar NULL,
	CONSTRAINT location_pkey PRIMARY KEY (city_id)
);
