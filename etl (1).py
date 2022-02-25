import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data +'/song_data////.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(["song_id","title", "artist_id", "year", "duration"]).dropDuplicates()
    songs_table.take(1)
    songs_table.printSchema()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'songs.parquet'), 'overwrite')
    songs_table.createOrReplaceTempView('songs')
    spark.sql("SELECT * FROM songs LIMIT 2").show()

    # extract columns to create artists table
    artists_table = df.select(["artist_id", "name", "location", "lattitude", "longitude"])
    artists_table.take(1) 
    artists_table.printSchema() 
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data,'artists.parquet'),'overwrite')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + '/log_data////.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    artists_table = df.select(["user_id" ,"first_name", "last_name", "gender", "level"])
    
    # write users table to parquet files
    artists_table.write.parquet(os.path.join(output_data,'users.parquet'),'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x:str(int(int(x)/1000)))
    df = df.withColumn('time_stamp',get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x:str(datetime.fromtimestamp(int(x)/1000.0)))
    df = df.withColumn('date_time',get_datetime(df.ts))
    
    # extract columns to create time table
    time_table = df.select(
                    col('datetime').alias('start_time'),
                    hour('datetime').alias('hour'),
                    dayofmonth('datetime').alias('day'),
                    weekofyear('datetime').alias('week'),
                    month('datetime').alias('month'),
                    year('datetime').alias('year'),
                    weekday('datetime').alias('weekday'))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year","month").parquet(os.path.join(output,'time.parquet'),'overwrite')

    # read in song data to use for songplays table
    song_df = spark.read.json(song_data)
    df = df.join(song_df, song_df.title == df.song)

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.select('start_time','user_id','level','song_id','session_id','location','user_agent',
                        month('date_time').AS('month'),year('start_time').AS('year')).withColumn("songplay_id",monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'songplays.parquet'), 'overwrite')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
