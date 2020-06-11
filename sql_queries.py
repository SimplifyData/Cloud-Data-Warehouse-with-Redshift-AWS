import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS factSongPlays"
user_table_drop = "DROP TABLE IF EXISTS dimUsers;"
song_table_drop = "DROP TABLE IF EXISTS dimSongs;"
artist_table_drop = "DROP TABLE IF EXISTS dimArtists;"
time_table_drop = "DROP TABLE IF EXISTS dimTime;"

# CREATE TABLES

staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events (
event_id INTEGER IDENTITY(0,1),
artist VARCHAR(50),
auth VARCHAR(20),
firstName VARCHAR(25),
gender VARCHAR(10),
itemInSession INTEGER,
lastName VARCHAR(25),
staging_length FLOAT,
staging_level VARCHAR(10),
location VARCHAR(50),
method VARCHAR(15),
page VARCHAR(15),
registration FLOAT,
sessionId INTEGER,
song VARCHAR(50),
status INTEGER,
ts INTEGER,
userAgent VARCHAR(200),
userId INTEGER
PRIMARY KEY (event_id)
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
song_id VARCHAR(20),
title VARCHAR(50),
duration FLOAT,
year INTEGER,
num_songs INTEGER,
artist_id VARCHAR(20),
artist_name VARCHAR(50),
artist_location VARCHAR(50),
artist_latitude FLOAT,
artist_longitude FLOAT,
PRIMARY KEY (song_id)
);
""")

songplay_table_create= ("""
CREATE TABLE IF NOT EXISTS factSongPlays (
sp_songplay_id INTEGER NOT NULL IDENTITY(0,1),
sp_start_time TIMESTAMP NOT NULL REFERENCES dimTIME(t_start_time),
sp_user_id INTEGER NOT NULL REFERENCES dimUsers(u_user_id),
sp_level VARCHAR(10) NOT NULL REFERENCES dimUsers(u_level),
sp_song_id VARCHAR(20) NOT NULL REFERENCES dimSongs(s_songs_id),
sp_artist_id VARCHAR(20) NOT NULL REFERENCES dimArtists(a_artist_id),
sp_session_id INTEGER NOT NULL SORTKEY,
sp_location VARCHAR(50) NOT NULL,
sp_user_agent VARCHAR(200) NOT NULL 
)
DISTSTYLE EVEN;
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS dimUsers (
u_user_id INTEGER NOT NULL IDENTITY(0,1) SORTKEY,
u_first_name VARCHAR(25) NOT NULL,
u_last_name VARCHAR(25) NOT NULL,
u_gender VARCHAR(10),
u_level VARCHAR(10)
) 
DISTSTYLE ALL;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS dimSongs (
s_song_id VARCHAR(20) NOT NULL,
s_title VARCHAR(50) NOT NULL,
s_artist_id VARCHAR(20) NOT NULL SORTKEY,
s_year INTEGER,
s_duration INTEGER
)
DISTSTYLE ALL;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS dimArtists (
a_artist_id VARCHAR(20) NOT NULL SORTKEY,
a_artist_name VARCHAR(50) NOT NULL,
a_artist_location VARCHAR(50),
a_artist_latitude FLOAT,
a_artist_longitude FLOAT
)
DISTSTYLE ALL;


""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS dimTime (
t_start_time TIMESTAMP NOT NULL SORTKEY,
t_hour INTEGER NOT NULL,
t_day INTEGER NOT NULL,
t_week INTEGER NOT NULL,
t_month INTEGER NOT NULL,
t_year INTEGER NOT NULL,
t_weekday INTEGER NOT NULL 
)
DISTSTYLE ALL;
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events 
FROM '{}'
CREDENTIALS 'aws_iam_role = {}'
gzip region 'us-west-2'
FORMAT AS JSON '{}';
""").format(config.get('S3_PATH','LOG_DATA'),
            config.get('IAM_ROLE','ARN'),
            config.get('S3_PATH','LOG_JSON_PATH'))

staging_songs_copy = ("""
COPY staging_songs
FROM '{}'
CREDENTIALS 'aws-iam_role = {}'
gzip region 'us-west-2'
JSON 'auto'
""").format(config.get('S3_PATH','LOG_DATA'),
            config.get('IAM_ROLE','ARN'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO factSongPlays (
sp_start_time,
sp_user_id,
sp_level,
sp_song_id,
sp_artist_id,
sp_session_id,
sp_location,
sp_user_agent)

SELECT staging_events.ts as sp_start_time,
staging_events.userId as sp_user_id,
staging_events.staging_level as sp_level,
staging_songs.song_id as sp_song_id,
staging_songs.artist_id as sp_artist_id,
staging_events.sessionId as sp_session_id,
staging_events.location as sp_location,
staging_events.userAgent as sp_user_agent

FROM staging_events
LEFT JOIN staging_songs
ON staging_events.artist = staging_songs.artist_name
AND staging_events.song = staging_songs.title

LEFT OUTER JOIN factSongPlays
ON staging_events.userId = factSongPlays.sp_user_id
AND staging_events.ts = factSongPlays.sp_start_time
WHERE staging_events.page = 'NextSong'
AND sp_start_time IS NOT NULL 
AND sp_user_id IS NOT NULL 
AND sp_level IS NOT NULL 
AND sp_song_id IS NOT NULL 
AND sp_artist_id IS NOT NULL 
AND sp_session_id IS NOT NULL 
AND sp_location IS NOT NULL 
AND sp_user_agent IS NOT NULL 
AND factSongPlays.songplay_id IS NULL
ORDER BY sp_start_time DESC, sp_user_id 

""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
