# Summary of Project :
- A startup called Sparkify wants to analyze the data they've been collecting on songs and user activities on their new music streaming app.
  For this analysis both log files of user activities and data containing information about songs present in app.

# Provided files and purpose of these files :
1. Directory of JSON logs - To analyse user activity
2. Directory with JSON metadata -  To get information about the songs present in this app

# The steps to perform this anlysis:
1. EXTRACT - Extract data from JSON Logs and JSON Metadata
2. TRANSFORM - Filtering records by "NextSong" Action. Select only required columns from dataframe.
3. LOAD - Load data into target tables i.e. Songplays, Songs, Users, Artists, Time.

# Information regarding files in repository :
1. Create_tables.py - This file contains queries to Drop and Create database, Drop tables if exists and Create tables. 

2. sql_queries.py - This file stores all queries in form of variables and these variables are then getting called in Create_tables.py for execution.

3. etl.ipynb - This file contains step by step instructions to build ETL.

4. etl.py - This file contains all the queries to load all the provided files (80 files in song_data and 32 files in log_data) 
            into dimension tables users, songs, artist, time and fact table songplays.

5. test.ipynb - This file is to test the data in tables.

# How to run python scripts?
1. Go to Terminal and then type command python create_tables.py and press enter. All the dimension and a fact table is created with this file.
2. Then type python etl.py and then press enter. After successful execution of this command, all files from log data and song data are load into fact and dimension table.

# Explanation of Queries
1. Selection of primary keys:
   Primary key on table column is useful for row level accessibilty.
   - Users Table :  Each user has unique identity. So User_ID was chosen as primary key.
   - Songs Table :  Each Song has unique identity. So Song_ID was chosen as primary key.
   -  Artists Table :  Each Artist has unique identity. So Artist_ID was chosen as primary key.
   - Times Table :  Each Start_time has unique identity. So Starttime was chosen as primary key.
   - Songplay_ID : When user plays any song on app, everytime new instance for songplay will be generated. So songplay_id will be primary    key. Since it will be generated and not inserted from any tables, datatype will be SERIAL.
   
2. Explanation of Queries:
   - Function create_database(): This function will drop database sparkifydb if already exists and creates new database. 
   
   - drop_tables(cur, conn): 
     This function has two arguments cursor and connection of database. In sql_queries.py file variable is defined called    drop_table_queries. This array consists of variables for tables which should be deleted. And each variable is defined in same file with drop table query. So by execution of this function, tables will be deleted if exists in database sparkify.
   
   - create_tables(cur, conn):
     This function has two arguments cursor and connection of database. In sql_queries.py file variable is defined called    create_table_queries. This array consists of variables for tables which should be created. And each variable is defined in same file with create table query. So by execution of this function, tables will be created in database sparkify.
   
   - process_song_file(cur, filepath):
     This function has two arguments cursor and filepath where song_files are stored. This function will read json file and insert records into songs and artist table.
     
   - process_log_file(cur, filepath):
     - This function has two arguments cursor and filepath where log files are stored. This function will read json file. Then records will be filtered by NextSong action. 
     - Timestamp column will be converted into datetime. With the help of convered datetime, start_time, hour, day, week, month, year, weekday are derived. The Time table will be populated with records.
     - User table will be populated with values of userId, firstName, lastName, gender, level in dataframe.
     - songplay table will be populated with values of ts, userId, level, songid, artistid, sessionId, location, userAgent. To get song ID and artist ID, songs and artists tables will be joined based on artist_id and particular values of song title, artist name, and duration of a song.
     
    - process_data(cur, conn, filepath, func):
      This function will process files present in filepath directory. The input aruguments required for execution for this function are cursor, connection, path where files are placed and functions process_song_file or process_log_file will be called to perform further processing of extracted files.