# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS Songplays"
user_table_drop = "DROP TABLE IF EXISTS Users"
song_table_drop = "DROP TABLE IF EXISTS Songs"
artist_table_drop = "DROP TABLE IF EXISTS Artist"
time_table_drop = "DROP TABLE IF EXISTS Times"

# CREATE TABLES

songplay_table_create = ("""CREATE TABLE Songplays(Songplay_ID Serial PRIMARY KEY,\
                                            Start_time bigint,\
                                            User_ID Varchar,\
                                            Level Varchar,\
                                            Song_ID Varchar,\
                                            Artist_ID Varchar,\
                                            Session_ID Varchar,\
                                            Location Varchar,\
                                            User_Agent Varchar,\
                                            FOREIGN KEY (Song_ID) REFERENCES Songs (Song_ID),\
                                            FOREIGN KEY (Artist_ID) REFERENCES Artist (Artist_ID),\
                                            FOREIGN KEY (User_ID) REFERENCES Users (User_ID)\
                                            )""")

user_table_create = ("""CREATE TABLE Users(User_ID Varchar PRIMARY KEY,\
                                       First_Name Varchar,\
                                       Last_Name Varchar,\
                                       Gender Varchar,\
                                       Level Varchar)""")

song_table_create = ("""CREATE TABLE Songs(Song_ID Varchar PRIMARY KEY,\
                                        Title Varchar,\
                                        Artist_ID Varchar,\
                                        Year Int,\
                                        Duration NUMERIC)""")

artist_table_create = ("""CREATE TABLE Artist(Artist_ID Varchar PRIMARY KEY,\
                                         Name Varchar,\
                                         Location Varchar,\
                                         Latitude NUMERIC,\
                                         Longitude NUMERIC)""")

time_table_create = ("""CREATE TABLE Times(Start_time bigint PRIMARY KEY,\
                                       Hour int,\
                                       Day int,\
                                       week int,\
                                       month int,\
                                       year int,\
                                       weekday int)""")

# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO Songplays(Start_time, User_ID, Level, Song_ID, Artist_ID,Session_ID, Location, User_Agent) VALUES(%s,%s,%s,%s,%s,%s,%s,%s) """)

user_table_insert = ("""INSERT INTO Users(User_ID, First_Name, Last_Name,Gender, Level) VALUES(%s,%s,%s,%s,%s) ON CONFLICT (User_ID) DO UPDATE SET Level = excluded.Level""")

song_table_insert = ("""INSERT INTO Songs(Song_ID, Title, Artist_ID, Year, Duration) VALUES(%s, %s, %s, %s, %s) ON CONFLICT (Song_ID) DO NOTHING""")

artist_table_insert = ("""INSERT INTO Artist(Artist_ID, Name, Location, Latitude, Longitude) VALUES(%s,%s,%s,%s,%s) ON CONFLICT (Artist_ID) DO NOTHING""")

time_table_insert = ("""INSERT INTO Times(Start_time, Hour, Day, week, month, year, weekday) VALUES(%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (Start_time) DO NOTHING""")

# FIND SONGS

song_select = ("""SELECT DISTINCT songs.Song_ID, artist.Artist_ID FROM songs INNER JOIN artist ON (songs.Artist_ID = artist.Artist_ID) WHERE songs.Title =%s AND artist.Name = %s AND songs.Duration = %s""")

# QUERY LISTS

create_table_queries = [user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [user_table_drop, song_table_drop, artist_table_drop, time_table_drop,songplay_table_drop]