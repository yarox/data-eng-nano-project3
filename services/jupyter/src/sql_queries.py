import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


# DROP TABLES
staging_events_table_drop = 'DROP TABLE IF EXISTS staging_events;'
staging_songs_table_drop = 'DROP TABLE IF EXISTS staging_songs;'
songplay_table_drop = 'DROP TABLE IF EXISTS songplays;'
user_table_drop = 'DROP TABLE IF EXISTS users;'
song_table_drop = 'DROP TABLE IF EXISTS songs;'
artist_table_drop = 'DROP TABLE IF EXISTS artists;'
time_table_drop = 'DROP TABLE IF EXISTS time;'


# CREATE TABLES
staging_events_table_create = '''
CREATE TABLE IF NOT EXISTS staging_events (
    artist        TEXT,
    auth          TEXT,
    firstName     TEXT,
    gender        TEXT,
    iteminsession INTEGER,
    lastname      TEXT,
    length        FLOAT,
    level         TEXT,
    location      TEXT,
    method        TEXT,
    page          TEXT,
    registration  FLOAT,
    sessionid     INTEGER,
    song          TEXT,
    status        INTEGER,
    ts            BIGINT,
    useragent     TEXT,
    userid        INTEGER
);
'''

staging_songs_table_create = '''
CREATE TABLE IF NOT EXISTS staging_songs (
    song_id          TEXT PRIMARY KEY,
    title            TEXT,
    duration         INTEGER,
    year             FLOAT,
    num_songs        FLOAT,
    artist_id        TEXT,
    artist_name      TEXT,
    artist_latitude  FLOAT,
    artist_longitude FLOAT,
    artist_location  TEXT
);
'''

songplay_table_create = '''
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
    start_time  BIGINT NOT NULL REFERENCES time(start_time) SORTKEY,
    user_id     INTEGER NOT NULL REFERENCES users(user_id),
    level       TEXT NOT NULL,
    song_id     TEXT NOT NULL REFERENCES songs(song_id) DISTKEY,
    artist_id   TEXT NOT NULL REFERENCES artists(artist_id),
    session_id  INTEGER NOT NULL,
    location    TEXT NOT NULL,
    user_agent  TEXT NOT NULL
);
'''

user_table_create = '''
CREATE TABLE IF NOT EXISTS users (
    user_id    INTEGER NOT NULL PRIMARY KEY,
    first_name TEXT NOT NULL,
    last_name  TEXT NOT NULL,
    gender     TEXT NOT NULL,
    level      TEXT NOT NULL
)
DISTSTYLE ALL;
'''

song_table_create = '''
CREATE TABLE IF NOT EXISTS songs (
    song_id   TEXT PRIMARY KEY,
    title     TEXT NOT NULL,
    artist_id TEXT NOT NULL REFERENCES artists(artist_id) SORTKEY DISTKEY,
    year      INTEGER,
    duration  FLOAT
);
'''

artist_table_create = '''
    CREATE TABLE IF NOT EXISTS artists (
    artist_id TEXT PRIMARY KEY,
    name      TEXT NOT NULL,
    location  TEXT,
    lattitude FLOAT,
    longitude FLOAT
)
DISTSTYLE ALL;
'''

time_table_create = '''
CREATE TABLE IF NOT EXISTS time (
    start_time BIGINT PRIMARY KEY,
    hour       INTEGER,
    day        INTEGER,
    week       INTEGER,
    month      INTEGER,
    year       INTEGER,
    weekday    INTEGER
)
DISTSTYLE ALL;
'''


# STAGING TABLES
staging_events_copy = f"""
COPY staging_events
FROM '{config.get('S3', 'LOG_DATA')}'
CREDENTIALS 'aws_iam_role={config.get('IAM_ROLE', 'ARN')}'
TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
REGION 'us-west-2'
FORMAT AS JSON '{config.get('S3', 'LOG_JSONPATH')}';
"""

staging_songs_copy = f"""
COPY staging_songs
FROM '{config.get('S3', 'SONG_DATA')}'
CREDENTIALS 'aws_iam_role={config.get('IAM_ROLE', 'ARN')}'
TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
REGION 'us-west-2'
JSON 'auto';
"""


# FINAL TABLES
songplay_table_insert = '''
INSERT INTO songplays (
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent
)

SELECT
    se.ts AS start_time,
    se.userid AS user_id,
    se.level,
    ss.song_id,
    ss.artist_id,
    se.sessionid AS session_id,
    se.location,
    se.useragent AS user_agent

FROM staging_events AS se
    LEFT JOIN staging_songs AS ss
        ON  se.song   = ss.title
        AND se.artist = ss.artist_name

LEFT OUTER JOIN songplays as sp
    ON  se.userid = sp.user_id
    AND se.ts     = sp.start_time

WHERE
    se.page            = 'NextSong'
    AND se.userid      is not NULL
    AND se.level       is not NULL
    AND ss.song_id     is not NULL
    AND ss.artist_id   is not NULL
    AND se.sessionid   is not NULL
    AND se.location    is not NULL
    AND se.useragent   is not NULL
    AND sp.songplay_id is NULL

ORDER BY start_time, user_id;
'''

user_table_insert = '''
INSERT INTO users (
    user_id,
    first_name,
    last_name,
    gender,
    level
)
SELECT DISTINCT
    userId AS user_id,
    firstName AS first_name,
    lastName AS last_name,
    gender,
    level
FROM staging_events
WHERE userId IS NOT NULL;
ORDER BY userId;
'''

song_table_insert = '''
INSERT INTO songs (
    song_id,
    title,
    artist_id,
    year,
    duration
)
SELECT DISTINCT
    song_id AS song_id,
    title AS title,
    artist_id AS artist_id,
    year,
    duration
FROM staging_songs
WHERE song_id IS NOT NULL;
'''

artist_table_insert = '''
INSERT INTO artists (
    artist_id,
    name,
    location,
    latitude,
    longitude,
)
SELECT DISTINCT
    artist_id,
    artist_name AS name,
    location,
    latitude,
    longitude
FROM staging_songs
WHERE artist_id IS NOT NULL;
'''

time_table_insert = '''
INSERT INTO dim_time (
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday
)
SELECT DISTINCT
    ts AS start_time,
    EXTRACT(hour FROM ts),
    EXTRACT(day FROM ts),
    EXTRACT(week FROM ts),
    EXTRACT(month FROM ts),
    EXTRACT(year FROM ts),
    EXTRACT(weekday FROM ts)
FROM staging_events
WHERE ts IS NOT NULL;
'''


# QUERY LISTS
create_table_queries = [user_table_create, artist_table_create, song_table_create, time_table_create, staging_events_table_create, staging_songs_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, artist_table_insert, song_table_insert, time_table_insert, songplay_table_insert]
