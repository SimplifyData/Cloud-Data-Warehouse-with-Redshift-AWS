import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table;"
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
length FLOAT,
level VARCHAR(10),
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

)
DISTSTYLE ALL;


""")

time_table_create = ("""
""")

# STAGING TABLES

staging_events_copy = ("""
""").format()

staging_songs_copy = ("""
""").format()

# FINAL TABLES

songplay_table_insert = ("""
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
