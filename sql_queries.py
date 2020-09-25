import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')
ARN = config.get('IAM_ROLE', 'ARN')

# DROP TABLES

staging_events_table_drop = "drop table if exists staging_event "
staging_songs_table_drop = "drop table if exists staging_song "
songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# CREATE TABLES

staging_events_table_create= ("""

 create table if not exists staging_event
 (artist          text,
  auth            text,
  first_name      text,
  gender          varchar,
  item_in_session     integer,
  last_name           text,
  length              float,
  level               text,
  location            text,
  method              text,
  page                text,
  registration        bigint,
  session_id          bigint,
  song                text,
  status              integer,
  ts                  bigint,
  user_agent          text,
  user_id             integer);

""")

staging_songs_table_create = ("""
 create table if not exists staging_song
 (
 num_songs          integer,
 artist_id          text,
 artist_latitude    float,
 artist_longitude   float,
 artist_location    text,
 artist_name        text,
 song_id            text,
 title              text,
 duration           float,
 year               integer
);
""")

songplay_table_create = ("""
    create table if not exists songplays 
    (songplay_id bigint identity(0, 1) sortkey,
    start_time bigint, 
    user_id text, 
    level text, 
    song_id text, 
    artist_id text, 
    session_id text, 
    location text,
    user_agent text,
    primary key(start_time)
     )
    diststyle auto;
""")

user_table_create = ("""
    create table if not exists users(
    user_id text sortkey,
    first_name text,
    last_name text,
    level text,
    gender text,
    primary key(user_id)
    )
    diststyle auto;

""")

song_table_create = ("""
    create table if not exists songs(
    song_id text,
    title text not null,
    artist_id text,
    year int,
    duration numeric,
    primary key(song_id)
    )
    diststyle auto;
""")

artist_table_create = ("""
    create table if not exists artists(
    artist_id text sortkey,
    name varchar not null,
    location varchar,
    latitude numeric,
    longitude numeric,
    primary key(artist_id)
    )
    diststyle auto;
""")

time_table_create = ("""
    create table if not exists time(
    start_time bigint,
    hour int not null,
    day int not null,
    week int not null,
    month int not null, 
    year int not null,
    weekday int not null,
    primary key(start_time)
    )
    diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_event 
from {}
iam_role {}
json {};
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
copy staging_song 
from {}
iam_role {}
json 'auto';
""").format(SONG_DATA, ARN)

 

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays(
 start_time, user_id, artist_id, song_id, "level",  
    session_id, location, user_agent)
    SELECT e.ts, e.user_id, s.artist_id, s.song_id, e.level, e.session_id, e.location, e.user_agent
    FROM staging_event e
    LEFT JOIN staging_song s on (e.artist = s.artist_name AND s.title= e.song)
    WHERE e.page = 'NextSong';
""")

user_table_insert = ("""

INSERT INTO users(user_id, first_name, last_name, gender, "level")
SELECT  user_id, first_name, last_name, gender, level 
FROM staging_event
where page = 'NextSong' and user_id is not null  
ORDER BY ts;
    
""")

song_table_insert = ("""
INSERT INTO songs(song_id, title, artist_id, year, duration)
SELECT distinct song_id, title, artist_id, year, duration
FROM staging_song
WHERE song_id is not null;
""")
                     
artist_table_insert = ("""
                     
INSERT INTO artists(artist_id, name, location, latitude, longitude)
SELECT distinct artist_id, artist_name, artist_location, artist_longitude longitude,
artist_latitude latitude
FROM staging_song
WHERE artist_id is not null;
""")

time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT start_time,
extract(year from timestamp 'epoch' + (start_time/1000 * interval '1 second')) as "year",
extract(month from timestamp 'epoch' + (start_time/1000 * interval '1 second')) as "month",
extract(hour from timestamp 'epoch' + (start_time/1000 * interval '1 second')) as "week",
extract(weekday from timestamp 'epoch' + (start_time/1000 * interval '1 second')) as "weekday",
extract(day from timestamp 'epoch' + (start_time/1000 * interval '1 second')) as "day",
extract(hour from timestamp 'epoch' + (start_time/1000 * interval '1 second')) as "hour"
FROM songplays
WHERE start_time is not null;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
