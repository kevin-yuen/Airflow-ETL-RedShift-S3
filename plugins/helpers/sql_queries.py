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

    create_songs_staging_table = """
    CREATE TABLE IF NOT EXISTS songs_staging (
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
    CREATE TABLE IF NOT EXISTS events_staging (
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
    def check_row_cnt(ds, cnt_method='*', cond_stmt=''):
        row_count_sql = f"""SELECT COUNT({cnt_method}) FROM {ds}"""

        if cond_stmt != '':
            cond_stmt = f'WHERE {cond_stmt}'
            row_count_sql = row_count_sql + ' ' + cond_stmt

        return row_count_sql
    
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
    
    @staticmethod
    def drop_table(ds):
        drop_table_sql =  f"""
        DROP TABLE IF EXISTS {ds} CASCADE;
        """
        return drop_table_sql
    
    @staticmethod
    def truncate_table(ds):
        truncate_table_sql = f"""
        TRUNCATE TABLE {ds};
        """
        return truncate_table_sql

    create_songplays_fact_table = """
    CREATE TABLE IF NOT EXISTS songplays_fact (
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

    create_artists_dim_table = """
    CREATE TABLE IF NOT EXISTS artists_dim (
        artistid varchar(256) NOT NULL,
        name varchar(512),
        location varchar(512),
        lattitude numeric(18,0),
        longitude numeric(18,0)
    );
    """

    create_songs_dim_table = """
    CREATE TABLE IF NOT EXISTS songs_dim (
        songid varchar(256) NOT NULL,
        title varchar(512),
        artistid varchar(256),
        "year" int4,
        duration numeric(18,0)
    );
    """

    create_time_dim_table = """
    CREATE TABLE IF NOT EXISTS time_dim (
        start_time timestamp NOT NULL,
        "hour" int4,
        "day" int4,
        week int4,
        "month" varchar(256),
        "year" int4,
        weekday varchar(256)
    ) ;
    """

    create_users_dim_table = """
    CREATE TABLE IF NOT EXISTS users_dim (
        userid int4 NOT NULL,
        first_name varchar(256),
        last_name varchar(256),
        gender varchar(256),
        "level" varchar(256)
    );
    """
