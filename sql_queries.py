import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = ("""
   CREATE TABLE IF NOT EXISTS staging_events(
       artist        VARCHAR,
       auth          VARCHAR,
       firstName      VARCHAR(35),
       gender        CHAR(1),
       iteminSession INTEGER,
       lastName      VARCHAR(35),
       length        FLOAT,
       level         VARCHAR,
       location      VARCHAR,
       method        VARCHAR,
       page          VARCHAR,
       registration  FLOAT,
       sessionId     INTEGER,
       song          VARCHAR,
       status        INTEGER,
       ts            BIGINT,
       userAgent     VARCHAR,
       userId        INTEGER       
   );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs(
        num_songs               INTEGER,
        artist_id               VARCHAR,
        artist_latitude         FLOAT,
        artist_longitude        FLOAT,
        artist_location         VARCHAR,
        artist_name             VARCHAR,
        song_id                 VARCHAR,
        title                   VARCHAR,
        duration                FLOAT,
        year                    INTEGER
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays(
        songplay_id                 INTEGER IDENTITY(1,1) PRIMARY KEY,
        start_time                  TIMESTAMP NOT NULL distkey sortkey,
        user_id                     INTEGER NOT NULL,
        level                       VARCHAR,
        song_id                     VARCHAR,
        artist_id                   VARCHAR,
        session_id                  INTEGER,
        location                    VARCHAR,
        user_agent                  VARCHAR
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(
        user_id             INTEGER PRIMARY KEY     sortkey,
        first_name          VARCHAR(35),
        last_name           VARCHAR(35),
        gender              CHAR(1),
        level               VARCHAR
    )
    diststyle all;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs(
        song_id         VARCHAR PRIMARY KEY         sortkey,
        title           VARCHAR,
        artist_id       VARCHAR NOT NULL,
        year            INTEGER,
        duration        FLOAT
    )
    diststyle all;
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists(
        artist_id               VARCHAR PRIMARY KEY     sortkey,
        name                    VARCHAR,
        location                VARCHAR,
        latitude                FLOAT,
        longitude               FLOAT
    )
    diststyle all;
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time(
        start_time              TIMESTAMP PRIMARY KEY       distkey sortkey,
        hour                    INTEGER,
        day                     INTEGER,
        week                    INTEGER,
        month                   INTEGER,
        year                    INTEGER,
        weekday                 VARCHAR(11)
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events 
    FROM {}
    iam_role {}
    FORMAT AS JSON {};
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    COPY staging_songs 
    FROM {}
    iam_role {}
    FORMAT AS JSON 'auto';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
        SELECT 
            TIMESTAMP 'epoch' + (se.ts/1000) * INTERVAL '1 second' as start_time,
            se.userId,
            se.level,
            ss.song_id,
            ss.artist_id,
            se.sessionId,
            se.location,
            se.userAgent
        FROM staging_events se
        JOIN staging_songs ss
        ON (se.song = ss.title AND se.artist = ss.artist_name)
        AND se.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users(user_id, first_name, last_name, gender, level)
        SELECT
            DISTINCT userId,
            firstName,
            lastName,
            gender,
            level
        FROM staging_events
        WHERE userId IS NOT NULL;
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT
        DISTINCT song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
    WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artists(artist_id, name, location, latitude, longitude)
    SELECT 
        DISTINCT artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time(start_time, hour, day, week, month, year, weekday)
    SELECT
        DISTINCT TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second' as start_time,
        EXTRACT(hour FROM start_time) as hour,
        EXTRACT(day FROM start_time) as day,
        EXTRACT(week FROM start_time) as week,
        EXTRACT(month FROM start_time) as month,
        EXTRACT(year FROM start_time) as year,
        TO_CHAR(start_time, 'Day' ) as weekday
    FROM staging_events
    WHERE start_time IS NOT NULL;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
