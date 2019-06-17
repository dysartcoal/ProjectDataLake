import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import max as sql_max
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import TimestampType, IntegerType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """Creates the spark session"""
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Read the JSON song data and create song and artist tables output in parquet format.
    
    Parameters:
    spark (SparkSession): The SparkSession for execution and management of tasks
    input_data  (String): Location of input data
    output_data (String): Location of output data 
    """
    # get filepath to song data file
    song_data = "song_data/*/*/*/*.json"
    outpath_songs = "songs"
    outpath_artists = "artists"
    
    
    # read song data file
    df = spark.read.json(os.path.join(input_data, song_data))
    
    # extract columns to create songs table
    songs_table = df["song_id", "title", "artist_id", "year", "duration"]
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write\
        .mode('overwrite')\
        .partitionBy("year", "artist_id")\
        .parquet(os.path.join(output_data, outpath_songs))
    

    # extract columns to create artists table
    artists_table = df.selectExpr("artist_id as artist_id", 
                                  "artist_name as name", 
                                  "artist_location as location", 
                                  "artist_latitude as latitude", 
                                  "artist_longitude as longitude")
    dd_artists_table = artists_table.drop_duplicates()
    
    # write artists table to parquet files
    dd_artists_table.write\
        .mode('overwrite')\
        .parquet(os.path.join(output_data, outpath_artists))


def process_log_data(spark, input_data, output_data):
    """Read the JSON log data and create songplays, users and time tables output in parquet format.
    
    Parameters:
    spark (SparkSession): The SparkSession for execution and management of tasks
    input_data  (String): Location of input data
    output_data (String): Location of output data
    """
    # get filepath to log data file
    log_data = "log_data/*/*/*.json"
    outpath_songplays = "songplays"
    outpath_users = "users"
    outpath_time = "time"
    song_table = "songs"
    artist_table = "artists"
    

    # read log data file
    df = spark.read.json(os.path.join(input_data, log_data))
    
    # filter by actions for song plays
    # update the ts value to be a timestamp
    df_songplays = df.filter(df.page=="NextSong")\
                    .withColumn('ts', (df.ts/1000).cast(dataType=TimestampType()))

    # user_id, first_name, last_name, gender, level
    # extract columns for users table    
    users_table = df_songplays.selectExpr("userId as user_id",
                                             "firstName as first_name",
                                             "lastName as last_name",
                                              "gender",
                                              "level"
                                             )\
                             .distinct()
    
    # there are users that are on both the free and paid levels
    # filter out those duplicates marked as being on the free level
    tb = users_table.selectExpr("user_id as user_id1", "level as level1")
    duplicate_users = users_table.join(tb, (~(users_table.level == tb.level1)) 
                                   & (users_table.user_id == tb.user_id1) 
                                   & (users_table.level==u'paid'))\
                            .select("user_id1", "level1")
    dd_users_table = users_table.join(duplicate_users, 
                                (users_table.user_id == duplicate_users.user_id1) 
                                & (users_table.level == duplicate_users.level1), 
                                how="left" )\
                .filter(col("level1").isNull())\
                .select("user_id", "first_name", "last_name", "gender", "level")
    
    # write users table to parquet files
    dd_users_table.write\
        .mode('overwrite')\
        .parquet(os.path.join(output_data, outpath_users))
    
    # extract columns to create time table
    time_table = df_songplays.withColumn('str_weekday', date_format(df_songplays.ts, 'EEEE'))\
                            .selectExpr("ts as start_time",
                                     "hour(ts) as hour",
                                     "day(ts) as day",
                                     "weekofyear(ts) as week",
                                     "month(ts) as month",
                                     "year(ts) as year",
                                     "str_weekday as weekday")\
                            .distinct()
    
    # write time table to parquet files partitioned by year and month
    time_table.write\
        .mode('overwrite')\
        .partitionBy("year", "month")\
        .parquet(os.path.join(output_data, outpath_time))

     # read in the song data columns required to populate the songplays table
    df_songs = spark.read.parquet(os.path.join(output_data, song_table))\
                        .select("song_id", "title", "artist_id")

    # read in artist data to use for songplays table in case of duplicate song titles
    df_artists = spark.read.parquet(os.path.join(output_data, artist_table))\
                        .selectExpr("artist_id as artist_id1", "name")

    # extract columns from joined song and log datasets to create songplays table 
    df_songartists = df_songs.join(df_artists,
                                   df_songs.artist_id == df_artists.artist_id1)\
                            .select("song_id", "title", "artist_id", "name")
    songplays1 = df_songplays.join(df_songartists,
                                   (df_songplays.song == df_songartists.title) 
                                   & (df_songplays.artist == df_songartists.name),
                                   "left")\
                            .selectExpr("ts as start_time",
                                        "userId as user_id",
                                        "level",
                                        "song_id",
                                        "artist_id",
                                        "sessionId as session_id",
                                        "location",
                                        "userAgent as user_agent")
    songplays_table = songplays1.withColumn("songplay_id", monotonically_increasing_id())\
                                .withColumn("year", date_format(songplays1.start_time, 'YYYY')\
                                            .cast(IntegerType()))\
                                .withColumn("month", date_format(songplays1.start_time, 'M')\
                                            .cast(IntegerType()))
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write\
        .mode('overwrite')\
        .partitionBy("year", "month")\
        .parquet(os.path.join(output_data, outpath_songplays))


def main():
    """Configure the input and output locations and call the processing methods"""
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://dysartcoal-dend-uswest2/analytics"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()


