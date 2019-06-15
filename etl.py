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
    print("Songs table count is {}".format(songs_table.count()))
    
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
        
    print("Pre-deduplicate artists count is  {}".format(artists_table.count()))
    print("Post-deduplicate artists count is {}".format(dd_artists_table.count()))
    
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
    # log_data = "log_data/*/*/*.json"
    log_data = "log_data/*.json"
    outpath_songplays = "songplays"
    outpath_users = "users"
    outpath_time = "time"
    song_table = "songs"
    artist_table = "artists"
    

    # read log data file
    df = spark.read.json(os.path.join(input_data, log_data))
    print("Total number of log rows is {}".format(df.count()))
    
    # filter by actions for song plays
    # update the ts value to be a timestamp
    df_songplays = df.filter(df.page=="NextSong")\
                    .withColumn('ts', (df.ts/1000).cast(dataType=TimestampType()))
    
    print("Total number of songplays rows is {}".format(df_songplays.count()))

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
    users_table.createOrReplaceTempView("tab_users")
    duplicate_users = spark.sql("""
        SELECT t1.user_id as user_id1,
                'free' as level1
        FROM tab_users t1 
        JOIN tab_users t2
        ON t1.user_id = t2.user_id
        AND t1.level='free'
        AND t2.level='paid'
        """)
    
    dd_users_table = users_table.join(duplicate_users, 
                                (users_table.user_id == duplicate_users.user_id1) 
                                & (users_table.level == duplicate_users.level1), 
                                how="left" )\
                .filter(col("level1").isNull())\
                .select("user_id", "first_name", "last_name", "gender", "level")
    print("Total number of users rows is {}".format(dd_users_table.count()))
    
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
    print("Total number of time rows is {}".format(time_table.count()))
    
    # write time table to parquet files partitioned by year and month
    time_table.write\
        .mode('overwrite')\
        .partitionBy("year", "month")\
        .parquet(os.path.join(output_data, outpath_time))

     # read in the song data columns required to populate the songplays table
    df_songs = spark.read.parquet(os.path.join(output_data, song_table))\
                        .select("song_id", "title", "artist_id")
    print("Total number of song rows is {}".format(df_songs.count()))

    # read in artist data to use for songplays table in case of duplicate song titles
    df_artists = spark.read.parquet(os.path.join(output_data, artist_table))\
                            .select("artist_id", "name")
    print("Total number of artist rows is {}".format(df_artists.count()))

    # extract columns from joined song and log datasets to create songplays table 
    df_songs.createOrReplaceTempView("tmp_songs")
    df_artists.createOrReplaceTempView("tmp_artists")
    df_songplays.createOrReplaceTempView("tmp_songplays")
    songplays1 = spark.sql("""
        SELECT sp.ts as start_time,
                sp.userId as user_id, 
                sp.level as level, 
                sa.song_id as song_id, 
                sa.artist_id as artist_id, 
                sp.sessionId as session_id,
                sp.location as location, 
                sp.userAgent as user_agent
        FROM tmp_songplays sp 
        LEFT JOIN
                (SELECT s.song_id, s.title, a.artist_id, a.name
                FROM tmp_songs s
                JOIN tmp_artists a
                ON s.artist_id = a.artist_id) sa
        ON sp.song = sa.title AND sp.artist = sa.name
        """)
        
    songplays_table = songplays1.withColumn("songplay_id", monotonically_increasing_id())\
                                .withColumn("year", date_format(songplays1.start_time, 'YYYY')\
                                            .cast(IntegerType()))\
                                .withColumn("month", date_format(songplays1.start_time, 'M')\
                                            .cast(IntegerType()))\

    print("Total number of songplays rows is {}".format(songplays_table.count()))
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write\
        .mode('overwrite')\
        .partitionBy("year", "month")\
        .parquet(os.path.join(output_data, outpath_songplays))


def main():
    """Configure the input and output locations and call the processing methods"""
    spark = create_spark_session()
    #input_data = "s3a://udacity-dend/"
    #output_data = "s3a://dysartcoal-dend-uswest2/analytics"
    input_data = "data"
    output_data = "analytics"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

