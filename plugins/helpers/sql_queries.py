class SqlQueries:
    songplay_table_insert = ("""
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)

    drop_songs_staging_table = """
    DROP TABLE IF EXISTS songs_staging;
    """

    drop_events_staging_table = """
    DROP TABLE IF EXISTS events_staging;
    """

    create_songs_staging_table = """
    CREATE TABLE songs_staging (
        song_id VARCHAR,
        num_songs INT4,
        title VARCHAR,
        artist_name VARCHAR,
        artist_latitude NUMERIC,
        year INT4,
        duration NUMERIC,
        artist_id VARCHAR,
        artist_longitude NUMERIC, 
        artist_location VARCHAR
    );
    """

    create_events_staging_table = """
    CREATE TABLE events_staging (
        artist VARCHAR,
        auth VARCHAR,
        firstName VARCHAR,
        gender VARCHAR,
        itemInSession INT4,
        lastName VARCHAR,
        length NUMERIC,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration NUMERIC,
        sessionId INT4,
        song VARCHAR,
        status INT4,
        ts INT8,
        userAgent VARCHAR,
        userId INT4
    );
    """