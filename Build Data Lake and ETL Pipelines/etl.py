#Importing directories
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types  import TimestampType 
import psycopg2

#Reading configuration file dl.cfg
config = configparser.ConfigParser()
config.read('dl.cfg')

#Get AWS IAM user's Key_ID and Access_Key
os.environ['AWS_ACCESS_KEY_ID']=config.get("AWS","AWS_ACCESS_KEY_ID")
os.environ['AWS_SECRET_ACCESS_KEY']=config.get("AWS","AWS_SECRET_ACCESS_KEY")


#Creating spark session
def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.5") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    The function process_song_data has three arguments.
    spark : Sparksession parameter
    input_data : The input path file which needs to be read and processed.
    output_data : The putput location where parquet file needs to be written.
    """
    
    # get filepath to song data file
    print('Getting song data file path')
    song_data = "s3a://udacity-dend/song_data/*/*/*/*.json"
    
    """Step 1 :  Loading Dimension Table Songs"""
    # read song data file
    print('Reading song data file')
    df_song_data = spark.read.json(song_data)

    
    # create temporary view of dataframe
    print('Creating temporary view song_data')
    df_song_data.createOrReplaceTempView("song_data")

    # extract columns to create songs table
    print('Extracting columns from song_data dataframe to create song table')
    songs_table = spark.sql("""
    SELECT distinct song_id, title, artist_id, year, duration
    FROM song_data""")
    
    # write songs table to parquet files partitioned by year and artist
    print('Writting song table to parquet files')
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet( os.path.join(output_data ,'songs'))
    print('Writing song table to parquet files is completed')

    """Step 2 :  Loading Dimension Table Artists"""
    # extract columns to create artists table
    print('Extracting data for Artists table')
    artists_table = spark.sql("""
    SELECT distinct artist_id,artist_name,artist_location,artist_latitude,artist_longitude
    FROM song_data""")
    
    # write artists table to parquet files
    print('Writing artists table to parquet files')
    artists_table.write.mode('overwrite').parquet( os.path.join(output_data ,'artists'))
    print('Writing artists table to parquet files is completed')

def process_log_data(spark, input_data, output_data):
    """
    The function process_song_data has three arguments.
    spark : Sparksession parameter
    input_data : The input path file which needs to be read and processed.
    output_data : The putput location where parquet file needs to be written.
    """
    
    # get filepath to log data file
    print('Getting log data file path')
    log_data = os.path.join(input_data, "log_data/*/*/*.json")

    # read log data file
    print('Reading log data file')
    df_log_data = spark.read.json(log_data)

    
    # filter by actions for song plays
    print('Filtering log data with action of NextSong')
    df_log_data_filtered = df_log_data[df_log_data.page=="NextSong"]
    
    print('Creating temporary view of filtered log data.')
    df_log_data_filtered.createOrReplaceTempView("log_data_ftr")

    """Step 3 :  Loading Dimension Table Users"""
    # extract columns for users table
    print('Extracting columns for Users table')
    users_table = spark.sql("""
    SELECT distinct userId,firstName,lastName,gender,level
    FROM log_data_ftr """)
    
    # write users table to parquet files
    print('Writing users table to parquet files')
    users_table.write.mode('overwrite').parquet( os.path.join(output_data ,'users'))
    print('Writing users table to parquet files is completed')

    """Step 4 :  Loading Dimension Table Time"""
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp((x/1000.0)), TimestampType())
    
    # Add new column to dataframe with new timestamp column calculated with the help of above udf
    print('Timestamp conversion')
    df_log_data_filtered = df_log_data_filtered.withColumn("newts", get_timestamp(df_log_data_filtered.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp((x/1000.0)).strftime('%Y-%m-%d %H:%M:%S'))
    
    # Add new column to dataframe with new datetime calculated with the help of above udf
    print('Datetime conversion')
    df_log_data_filtered = df_log_data_filtered.withColumn("datetime", get_datetime(df_log_data_filtered.ts))
    
    # extract columns to create time table
    print('Creating temporary view of time_data.')
    df_log_data_filtered.createOrReplaceTempView("time_data")
    
    print('Extracting and creating columns for time table')
    time_table = spark.sql("""
                            SELECT ts as start_time,
                                   hour(datetime) as hour,
                                   dayofmonth(datetime) as day,
                                   weekofyear(datetime) as week,
                                   month(datetime) as month,
                                   year(datetime) as year,
                                   weekday(datetime) as weekday
                              FROM time_data """)
    
    # write time table to parquet files partitioned by year and month
    print('Writing time table to parquet files partitioned by year and month.')
    time_table.write.mode('overwrite').partitionBy("year", "month").parquet( os.path.join(output_data ,'time'))
    print('Writing time table to parquet files is completed')
    
    """Step 5 :  Loading Fact Table Songplays"""
    # extract columns from joined song and log datasets to create songplays table
    
    print('Extracting and creating columns for Songplays table')
    songplays_table = spark.sql("""
    SELECT 
         ROW_NUMBER() OVER (ORDER BY start_time,user_id,level,song_id,artist_id,session_id,location,user_agent) as songplay_id
        ,start_time
        ,month
        ,year
        ,user_id
        ,level
        ,song_id
        ,artist_id
        ,session_id
        ,location
        ,user_agent
      from 
            (select distinct
                    to_timestamp(log.ts/1000) as start_time
                   ,month(to_timestamp(log.ts/1000)) as month
                   ,year(to_timestamp(log.ts/1000)) as year
                   ,log.userid as user_id
                   ,log.level as level
                   ,song.song_id as song_id
                   ,song.artist_id as artist_id
                   ,log.sessionid as session_id
                   ,log.location as location
                   ,log.useragent as user_agent

                  FROM        log_data_ftr log 
                    left join song_data song
                         on    log.song = song.title
                           and log.length = song.duration
                            ) log_join_song
                   """)

    # write songplays table to parquet files partitioned by year and month
    print('Writing songplays table to parquet files partitioned by year and month')
    songplays_table.write.mode('overwrite').partitionBy("year","month").parquet(os.path.join(output_data,'songplays'))
    print('Writing songplays table to parquet files is completed')


def main():
    try:
        print ('Executing function create spark session')
        spark = create_spark_session()
        print ('Spark session has been created successfully')
    
    except psycopg2.Error as e:
        print("error")
        print(e)
        conn.rollback()
    
    input_data = "s3a://udacity-dend/"
    
    output_data = "s3a://ywaranass3bucket/DataLake-Project/"
    
    try:
        print ('Excecuting function process_song_data')  
        process_song_data(spark, input_data, output_data)
        print ('Function process_song_data executed successfully')
    
    except psycopg2.Error as e:
        print("error")
        print(e)
        conn.rollback()
    
    
    try:
        print ('Excecuting function process_log_data')  
        process_log_data(spark, input_data, output_data)
        print ('Function process_log_data executed successfully')
    
    except psycopg2.Error as e:
        print("error")
        print(e)
        conn.rollback()


if __name__ == "__main__":
    main()
