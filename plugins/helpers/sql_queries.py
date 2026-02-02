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
        FROM events_staging
        WHERE page='NextSong') events
        LEFT JOIN songs_staging songs
        ON events.song = songs.title
            AND events.artist = songs.artist_name
            AND events.length = songs.duration
    """)

    user_table_insert = ("""
        SELECT DISTINCT
            userid,
            firstname,
            lastname,
            gender,
            level
        FROM events_staging
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT DISTINCT
            song_id, 
            title, 
            artist_id,
            year,
            duration
        FROM songs_staging
    """)

    artist_table_insert = ("""
        SELECT DISTINCT
            artist_id,
            artist_name,
            artist_location,
            artist_latitude,
            artist_longitude
        FROM songs_staging
    """)

    time_table_insert = ("""
        SELECT
            start_time,
            EXTRACT(hour FROM start_time),
            EXTRACT(day FROM start_time),
            EXTRACT(week FROM start_time), 
            EXTRACT(month FROM start_time),
            EXTRACT(year FROM start_time),
            EXTRACT(dayofweek FROM start_time)
        FROM songplays_fact
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
        num_songs INT,
        title VARCHAR,
        artist_name VARCHAR,
        artist_latitude FLOAT,
        year INT,
        duration FLOAT,
        artist_id VARCHAR,
        artist_longitude FLOAT, 
        artist_location VARCHAR
    );
    """

    create_events_staging_table = """
    CREATE TABLE events_staging (
        artist VARCHAR,
        auth VARCHAR,
        firstName VARCHAR,
        gender VARCHAR,
        itemInSession INT,
        lastName VARCHAR,
        length FLOAT,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration BIGINT,
        sessionId INT,
        song VARCHAR,
        status INT,
        ts BIGINT,
        userAgent VARCHAR,
        userId INT
    );
    """

    @staticmethod
    def copy_s3_to_staging(staging_table, src_path_data, iam_role, src_path_mapping):
        copy_s3_to_staging = f"""
        COPY {staging_table}
        FROM '{src_path_data}'
        IAM_ROLE '{iam_role}'
        FORMAT AS JSON '{src_path_mapping}'
        REGION 'us-east-1'
        """

        return copy_s3_to_staging
    
    @staticmethod
    def check_row_cnt(staging_table):
        row_count_sql = f"""SELECT COUNT(*) FROM {staging_table}"""

        return row_count_sql
    
    @staticmethod
    def check_nulls(staging_table, cond_stmt):
        # check nulls on critical columns
        null_cnt_sql = f"""
        SELECT COUNT(*)
        FROM {staging_table}
        WHERE {cond_stmt}
        """

        return null_cnt_sql
    
    @staticmethod
    def check_uniqueness(staging_table, cols):
        # check uniqueness on critical columns that form the composite natural key
        uniqueness_cnt_sql = f"""
        SELECT COUNT(*)
        FROM (
            SELECT {cols}
            FROM {staging_table}
            GROUP BY {cols}
            HAVING COUNT(*) > 1
        )
        """
        return uniqueness_cnt_sql
    
    drop_songplays_fact_table = """
    DROP TABLE IF EXISTS songplays_fact;
    """

    create_songplays_fact_table = """
    CREATE TABLE songplays_fact (
        playid varchar(32) NOT NULL,
        start_time timestamp NOT NULL,
        userid int4 NOT NULL,
        "level" varchar(256),
        songid varchar(256),
        artistid varchar(256),
        sessionid int4,
        location varchar(256),
        user_agent varchar(256)
    );
    """

