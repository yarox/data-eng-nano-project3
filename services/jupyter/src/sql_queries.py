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
"""

staging_songs_copy = f"""
"""


# FINAL TABLES
songplay_table_insert = '''
'''

user_table_insert = '''
'''

song_table_insert = '''
'''

artist_table_insert = '''
'''

time_table_insert = '''
'''


# QUERY LISTS
create_table_queries = [user_table_create, artist_table_create, song_table_create, time_table_create, staging_events_table_create, staging_songs_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, artist_table_insert, song_table_insert, time_table_insert, songplay_table_insert]
