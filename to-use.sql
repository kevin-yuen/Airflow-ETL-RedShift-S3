-- FACT_SONGPLAYS
	-- playid varchar(32) NOT NULL, --> generate
	-- start_time timestamp NOT NULL, --> events_staging.ts
	-- userid int4 NOT NULL, --> events_staging.userid
	-- "level" varchar(256), --> events_staging.level
	-- songid varchar(256), --> songs_staging.song_id
	-- artistid varchar(256), --> songs_staging.artist_id
	-- sessionid int4, --> events_staging.sessionid
	-- location varchar(256), --> events_staging.location
	-- user_agent varchar(256), --> events_staging.useragent
	-- CONSTRAINT songplays_pkey PRIMARY KEY (playid)

SELECT DISTINCT
	TIMESTAMP 'epoch' + (e.ts / 1000) * INTERVAL '1 second' AS start_time,
	e.userid,
	e.level,
    s.song_id,
	s.artist_id,
	e.sessionid,
	e.location,
	e.useragent AS user_agent
FROM songs_staging s
	INNER JOIN events_staging e
	ON s.artist_name = e.artist
	--AND s.title = e.song
WHERE page = 'NextSong'

-- DIM_ARTIST
	artistid varchar(256) NOT NULL, -- songs_staging.artist_id
	name varchar(512), -- songs_staging.artist_name (song_staging.artist_name == events_staging.artist)
	location varchar(512), -- songs_staging.artist_location
	latitude numeric(18,0), --songs_staging.artist_latitude
	longitude numeric(18,0) --songs_staging.artist_longitude

-- DIM_SONG
	songid varchar(256) NOT NULL, -- songs_staging.song_id
	title varchar(512), -- songs_staging.title
	artistid varchar(256), -- songs_staging.artist_id
	"year" int4, -- songs_staging.year
	duration numeric(18,0), -- songs_staging.duration
	CONSTRAINT songs_pkey PRIMARY KEY (songid)

-- DIM_USER
	userid int4 NOT NULL, -- events_staging.userid
	first_name varchar(256), -- events_staging.firstname
	last_name varchar(256), -- events_staging.lastname
	gender varchar(256), -- events_staging.gender
	"level" varchar(256), -- events_staging.level
	CONSTRAINT users_pkey PRIMARY KEY (userid)

-- DIM_TIME <-- user session
	start_time timestamp NOT NULL,
	"hour" int4,
	"day" int4,
	week int4,
	"month" varchar(256),
	"year" int4,
	weekday varchar(256),
	CONSTRAINT time_pkey PRIMARY KEY (start_time)

SELECT
    DISTINCT(ts),
    TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second' AS start_time,
    EXTRACT(hour FROM start_time)     AS hour,
    EXTRACT(day FROM start_time)      AS day,
    EXTRACT(week FROM start_time)     AS week,
    EXTRACT(month FROM start_time)    AS month,
    TRIM(TO_CHAR(start_time, 'Month')) AS month_name,
    EXTRACT(year FROM start_time)     AS year,
    EXTRACT(dow FROM start_time)      AS weekday,
    TRIM(TO_CHAR(start_time, 'Day')) AS weekday_name
FROM events_staging;


