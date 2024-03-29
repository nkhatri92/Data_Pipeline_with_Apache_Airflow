CREATE TABLE public.artists (
	artist_id varchar(256) NOT NULL,
	name varchar(256),
	location varchar(256),
	lattitude numeric(18,0),
	longitude numeric(18,0)
);

CREATE TABLE public.songplays (
	songplay_id varchar(32) NOT NULL,
	start_time timestamp NOT NULL,
	user_id int4 NOT NULL,
	"level" varchar(256),
	song_id varchar(256),
	artist_id varchar(256),
	session_id int4,
	location varchar(256),
	user_agent varchar(256),
	CONSTRAINT songplays_pkey PRIMARY KEY (songplay_id)
);

CREATE TABLE public.songs (
	song_id varchar(256) NOT NULL,
	title varchar(256),
	artist_id varchar(256),
	"year" int4,
	duration numeric(18,0),
	CONSTRAINT songs_pkey PRIMARY KEY (song_id)
);

CREATE TABLE public.staging_events (
	artist varchar(256),
	auth varchar(256),
	firstname varchar(256),
	gender varchar(256),
	iteminsession int4,
	lastname varchar(256),
	length numeric(18,0),
	"level" varchar(256),
	location varchar(256),
	"method" varchar(256),
	page varchar(256),
	registration numeric(18,0),
	sessionid int4,
	song varchar(256),
	status int4,
	ts int8,
	useragent varchar(256),
	userid int4
);


CREATE TABLE public.staging_songs (
	num_songs int4,
	artist_id varchar(256),
	artist_name varchar(256),
	artist_latitude numeric(18,0),
	artist_longitude numeric(18,0),
	artist_location varchar(256),
	song_id varchar(256),
	title varchar(256),
	duration numeric(18,0),
	"year" int4
);

CREATE TABLE public.time (
	start_time timestamp NOT NULL PRIMARY KEY,
    hour integer,
    day integer,
    week integer,
    month integer, 
    year integer,
    weekday integer, 
    weekday text)

CREATE TABLE public.users (
	user_id int4 NOT NULL,
	first_name varchar(256),
	last_name varchar(256),
	gender varchar(256),
	"level" varchar(256),
	CONSTRAINT users_pkey PRIMARY KEY (user_id)
);





