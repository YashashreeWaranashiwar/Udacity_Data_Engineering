import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events "
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS Songplays"
user_table_drop = "DROP TABLE IF EXISTS Users"
song_table_drop = "DROP TABLE IF EXISTS Songs"
artist_table_drop = "DROP TABLE IF EXISTS Artists"
time_table_drop = "DROP TABLE IF EXISTS Time"

# CREATE TABLES

staging_events_table_create= ("""\
    create table if not exists staging_events (\
                                            artist varchar(255) encode text255,
                                            auth varchar(255) encode text255,
                                            firstName varchar(100),
                                            gender varchar(1),
                                            itemInSession integer,
                                            lastName varchar(100),
                                            length DOUBLE PRECISION,
                                            level varchar(20),
                                            location varchar(255) encode text255,
                                            method varchar(10),
                                            page varchar(50),
                                            registration varchar(100),
                                            sessionId integer,
                                            song varchar(200),
                                            status integer,
                                            ts bigint,
                                            userAgent varchar(255) encode text255,
                                            userId integer)\
                                            diststyle even;
                                """)  


staging_songs_table_create = ("""\
    create table if not exists staging_songs (\
                                    num_songs integer,\
                                    artist_id varchar(100),\
                                    artist_latitude varchar(100),\
                                    artist_longitude varchar(100),\
                                    artist_location varchar(100),\
                                    artist_name varchar(100),\
                                    song_id varchar(100),\
                                    title varchar(100),\
                                    duration decimal,\
                                    year integer\
                                )\
                            """)


user_table_create = ("""CREATE TABLE if not exists Users(User_ID Varchar PRIMARY KEY,\
                                       First_Name Varchar,\
                                       Last_Name Varchar,\
                                       Gender Varchar,\
                                       Level Varchar)""")

song_table_create = ("""CREATE TABLE if not exists Songs(Song_ID Varchar PRIMARY KEY,\
                                        Title Varchar,\
                                        Artist_ID Varchar,\
                                        Year Int,\
                                        Duration NUMERIC)""")

artist_table_create = ("""CREATE TABLE if not exists Artists(Artist_ID Varchar PRIMARY KEY,\
                                         Name Varchar,\
                                         Location Varchar,\
                                         Latitude NUMERIC,\
                                         Longitude NUMERIC)""")

time_table_create = ("""CREATE TABLE if not exists Time(Start_time bigint PRIMARY KEY,\
                                       Hour int,\
                                       Day int,\
                                       week int,\
                                       month int,\
                                       year int,\
                                       weekday int)""")
songplay_table_create = ("""\
     CREATE TABLE if not exists Songplays(\
                                    Songplay_ID bigint identity(1,1),\
                                    Start_time bigint,\
                                    User_ID Varchar,\
                                    Level Varchar,\
                                    Song_ID Varchar,\
                                    Artist_ID Varchar,\
                                    Session_ID Varchar,\
                                    Location Varchar,\
                                    User_Agent Varchar,\
                                    FOREIGN KEY (Song_ID) REFERENCES Songs (Song_ID),\
                                    FOREIGN KEY (Artist_ID) REFERENCES Artists (Artist_ID),\
                                    FOREIGN KEY (User_ID) REFERENCES Users (User_ID))""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from {}
    iam_role {}
    region 'us-west-2' compupdate off 
    JSON {};
""").format(config.get('S3','LOG_DATA'),config.get('IAM_ROLE', 'ARN'),config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
copy staging_songs from {}
    iam_role {}
    region 'us-west-2' compupdate off 
    JSON 'auto' truncatecolumns;
""").format(config.get('S3','SONG_DATA'),config.get('IAM_ROLE', 'ARN'))


# FINAL TABLES

songplay_table_insert = ("""insert into songplays(Start_time, User_ID, Level, Song_ID, Artist_ID, Session_ID, Location, User_Agent)  (select distinct events.ts,events.userid, events.level, songs.song_id, songs.artist_id,events.sessionid, events.location, events.userAgent from staging_events events join songs songs on (events.song = songs.title and events.length = songs.duration ) join artists artists on (artists.artist_id = songs.artist_id and events.artist = artists.name) where events.page = 'NextSong')
""")

user_table_insert = ("""insert into users(select distinct events.userId, events.firstname, events.lastname, events.gender, events.level from staging_events events left join users on (events.userid = users.user_id) where events.page = 'NextSong' and users.user_id is null)
""")

song_table_insert = ("""insert into songs (select distinct song_id, title,artist_id,year,duration from staging_songs where song_id not in(select song_id from songs))
""")

artist_table_insert = ("""insert into Artists(select distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude from staging_songs where artist_id not in(select artist_id from artists))
""")

time_table_insert = ("""insert into Time (select\
                                            ts,\
                                            date_part(hour, start_time),\
                                            date_part(day, start_time),\
                                            date_part(week, start_time),\
                                            date_part(month, start_time),\
                                            date_part(year, start_time),\
                                            date_part(weekday, start_time)\
                                            from\
                                                (SELECT ts,TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time FROM staging_events))\
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
