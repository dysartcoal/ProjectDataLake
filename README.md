# Udacity DEND - Project 4: Spark Data Lake

## Background
Sparkify want to be able to query their user data to get more information on the songs that are being played.  By combining the logs (which capture the user and their song play events) with the song database an analytics platform for song play analysis can be implemented.

This etl workflow reads source song and log data from S3, processes the data into fact and dimension tables using Spark on the AWS EMR platform then writes the 5 output tables to S3 in parquet format which can then be used as the basis for analysis of the song play data.


## Overall Workflow

The workflow was developed locally and then run in a Jupyter Notebook with a connection to an AWS EMR cluster.  The overall workflow below describes the ETL workflow for the AWS EMR cluster implementation.

1. The song data is read from S3.
2. The song data is processed into two tables: songs and artists.
3. The songs table is written to parquet format on S3 partitioned by year and artist.  
4. The artists table is deduplicated and written to parquet format on S3.
5. The log data is read from S3.
6. The users and time tables are extracted from the log data.  Both tables have duplicates removed.
7. The users table is written to parquet format on S3.
8.  The time table is written to parquet format on S3 partitioned by year and month.
9. The song and artist tables are read from the parquet format files 
10. songs and artists data are joined to the log data to create the songplays table.
11. The songplays table is written to parquet format on S3 partitioned by year and month.

The result of the ETL process are the 4 dimensional tables (songs, artists, time, users) and the fact table (songplays) written to separate folders on S3 in parquet format ready for analysis.

The entity relationship diagram for the tables is shown below:

 ![Entity Relationship Diagram](datalakes_erd.png)

## Files

The following files form part of this project:

- **etl.py** : Run this python file to load the data from the source files from S3, process the data and write all 5 tables to parquet format on S3.
- **dl.cfg** : Holds the configuration settings for the ETL system.  


## The Development Process


Initially the python was written using a combination of SQL style and imperative use of pyspark.  

### Issue 1
This mixed approach worked without issue locally but caused an error "Unable to instantiate org.apache.hadoop.hive.ql.metadata.SessionHiveMetaStoreClient" when deployed in the EMR notebook.  I identified the use of SQL style queries as the cause of the problem however I wasn't able to find a configuration resolution so I re-wrote the queries using imperative style.

### Issue 2
On each run of the process_song_data() function on EMR an error was generated saying that the session was inactive.  I am unsure how many of the songs were not written to the parquet data but I was able to continue on to the process_log_data() function and process that data, along with the song and artist data that had been written successfully during the process_song_data phase.

It is expected that a solution could be sought to the problem with writing the song_data perhaps involving breaking the source data into smaller chunks for processing and writing to output or investigating any EMR system settings which may be the cause of the inactive session message.

## Conclusion

Despite two issues, one resolved and one put on hold for the meantime, the ETL system ran successfully on the AWS EMR cluster, processed the input files and wrote the files to output on S3 in parquet format for using as the basis for analysis of song play data.


