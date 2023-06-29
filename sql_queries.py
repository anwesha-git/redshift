import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')

DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")
DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")
DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")

DWH_DB_NAME                 = config.get("CLUSTER","DB_NAME")
DWH_DB_USER            = config.get("CLUSTER","DB_USER")
DWH_DB_PASSWORD        = config.get("CLUSTER","DB_PASSWORD")
DWH_DB_PORT               = config.get("CLUSTER","DB_PORT")

LOG_DATA          = config.get("S3", "LOG_DATA")  
LOG_PATH          = config.get("S3", "LOG_JSONPATH")    
SONG_DATA         = config.get("S3", "SONG_DATA")   

DWH_ARN           = config.get("IAM_ROLE", "ARN")  

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_event"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_song"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS user"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_songs_table_create= ("""
CREATE TABLE staging_song
(
        num_songs           INTEGER,
        artist_id           VARCHAR,
        artist_latitude     FLOAT,
        artist_longitude    FLOAT,
        artist_location     VARCHAR,
        artist_name         VARCHAR,
        song_id             VARCHAR,
        title               VARCHAR,
        duration            FLOAT,
        year                INTEGER
)
""")

staging_events_table_create = ("""
CREATE TABLE staging_event
(
        artist              VARCHAR,
        auth                VARCHAR,
        firstName           VARCHAR,
        gender              VARCHAR,
        itemInSession       INTEGER,
        lastName            VARCHAR,
        length              FLOAT,
        level               VARCHAR,
        location            VARCHAR,
        method              VARCHAR,
        page                VARCHAR,
        registration        FLOAT,
        sessionId           INTEGER,
        song                VARCHAR,
        status              INTEGER,
        ts                  TIMESTAMP,
        userAgent           VARCHAR,
        userId              INTEGER 
)
""")

songplay_table_create = ("""
CREATE TABLE songplay 
(
        songplay_id         INTEGER         IDENTITY(0,1)   PRIMARY KEY,
        start_time          TIMESTAMP       NOT NULL SORTKEY DISTKEY,
        user_id             INTEGER         NOT NULL,
        level               VARCHAR,
        song_id             VARCHAR         NOT NULL,
        artist_id           VARCHAR         NOT NULL,
        session_id          INTEGER,
        location            VARCHAR,
        user_agent          VARCHAR
)

""")

user_table_create = ("""
CREATE TABLE user 
(
        user_id             INTEGER         NOT NULL SORTKEY PRIMARY KEY,
        first_name          VARCHAR         NOT NULL,
        last_name           VARCHAR         NOT NULL,
        gender              VARCHAR         NOT NULL,
        level               VARCHAR         NOT NULL
)
""")

song_table_create = ("""
CREATE TABLE song
(
        song_id             VARCHAR         NOT NULL SORTKEY PRIMARY KEY,
        title               VARCHAR         NOT NULL,
        artist_id           VARCHAR         NOT NULL,
        year                INTEGER         NOT NULL,
        duration            FLOAT
)
""")

artist_table_create = ("""
CREATE TABLE artist 
(
        artist_id           VARCHAR         NOT NULL SORTKEY PRIMARY KEY,
        name                VARCHAR         NOT NULL,
        location            VARCHAR,
        latitude            FLOAT,
        longitude           FLOAT
)
""")

time_table_create = ("""
CREATE TABLE time 
(
        start_time          TIMESTAMP       NOT NULL DISTKEY SORTKEY PRIMARY KEY,
        hour                INTEGER         NOT NULL,
        day                 INTEGER         NOT NULL,
        week                INTEGER         NOT NULL,
        month               INTEGER         NOT NULL,
        year                INTEGER         NOT NULL,
        weekday             VARCHAR(20)     NOT NULL
)
""")

# STAGING TABLES COPY DATA

staging_events_copy = ("""
COPY staging_event
FROM '{}'
 credentials 'aws_iam_role={}'
format as json '{}'
region 'us-west-2'
dateformat 'auto'
""").format(LOG_DATA,LOG_PATH,DWH_ARN)

staging_songs_copy = ("""
COPY staging_song 
FROM '{}'
 credentials 'aws_iam_role={}'
format as json 'auto'
region 'us-west-2'
""").format(SONG_DATA,DWH_ARN)

# FACT & DIM TABLES INSERT

songplay_table_insert = ("""
    INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT  DISTINCT(se.ts)  AS start_time, 
            se.userId        AS user_id, 
            se.level         AS level, 
            ss.song_id       AS song_id, 
            ss.artist_id     AS artist_id, 
            se.sessionId     AS session_id, 
            se.location      AS location, 
            se.userAgent     AS user_agent
    FROM staging_event se
    JOIN staging_song  ss   ON (se.song = ss.title AND se.artist = ss.artist_name)
    AND se.page  =  'NextSong'
""")

user_table_insert = ("""
    INSERT INTO user (user_id, first_name, last_name, gender, level)
    SELECT  DISTINCT(se.userId)    AS user_id,
            se.firstName           AS first_name,
            se.lastName            AS last_name,
            se.gender,
            se.level
    FROM staging_event se
    WHERE se.user_id IS NOT NULL
    AND se.page  =  'NextSong'
""")

song_table_insert = ("""
    INSERT INTO song (song_id, title, artist_id, year, duration)
    SELECT  DISTINCT(ss.song_id) AS song_id,
            ss.title,
            ss.artist_id,
            ss.year,
            ss.duration
    FROM staging_song ss
    WHERE ss.song_id IS NOT NULL
""")

artist_table_insert = ("""
    INSERT INTO artist (artist_id, name, location, latitude, longitude)
    SELECT  DISTINCT(ss.artist_id) AS artist_id,
            ss.artist_name         AS name,
            ss.artist_location     AS location,
            ss.artist_latitude     AS latitude,
            ss.artist_longitude    AS longitude
    FROM staging_song ss
    WHERE ss.artist_id IS NOT NULL
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT  start_time                AS start_time,
            TIMESTAMP 'epoch' + CAST(start_time AS BIGINT)/1000 * INTERVAL '1 second' AS part_ts
            EXTRACT(hour FROM part_ts)       AS hour,
            EXTRACT(day FROM part_ts)        AS day,
            EXTRACT(week FROM part_ts)       AS week,
            EXTRACT(month FROM part_ts)      AS month,
            EXTRACT(year FROM part_ts)       AS year,
            EXTRACT(dayofweek FROM part_ts)  as weekday
    FROM songplay;
""")

#COUNTS OF RECORDS
count_staging_events = "SELECT COUNT(*) FROM staging_event;"
count_staging_songs = "SELECT COUNT(*) FROM staging_song;"
count_songplays = "SELECT COUNT(*) FROM songplay;"
count_songs = "SELECT COUNT(*) FROM song"
count_artists = "SELECT COUNT(*) FROM artist;"
count_time = "SELECT COUNT(*) FROM time;"
count_users = "SELECT COUNT(*) FROM user;"

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
select_counts = [count_staging_events, count_staging_songs, count_songplays, count_songs, count_artists, count_time, count_users]