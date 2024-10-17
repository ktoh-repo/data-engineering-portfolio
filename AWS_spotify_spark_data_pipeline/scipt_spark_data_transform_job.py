import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

from pyspark.sql.functions import explode, col, udf, when, size, concat_ws, collect_list, element_at, to_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from datetime import datetime
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

# need to change to os.environ
client_credentials_manager = SpotifyClientCredentials(client_id = '5f54ff961cbf41608b9c6eb7d7613e05', client_secret = '49622debd6274f2db7f52db6dad72de3')
spAPI = spotipy.Spotify(client_credentials_manager = client_credentials_manager)

s3_path = "s3://spotify-etl-project-tqx/raw_data/to_processed/"
source_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type = "s3",
    connection_options = {"paths": [s3_path]},
    format="json"
)

spotify_df = source_dyf.toDF()
df = spotify_df

def process_albums(df):
    df = df.withColumn("items", explode("items")).select(
        col("items.track.album.id").alias("album_id"),
        col("items.track.album.name").alias("album_name"),
        col("items.track.album.release_date").alias("release_date"),
        col("items.track.album.total_tracks").alias("total_tracks"),
        col("items.track.album.external_urls.spotify").alias("url")
    ).drop_duplicates(["album_id"])
    return df
   
   
def get_artist_info(artist_id):
    artist_info = []
    try:
        artist_info = spAPI.artist(artist_id)
        genres = ", ".join(artist_info["genres"])
        followers = artist_info['followers']['total']
        popularity = artist_info['popularity']
        return genres, followers, popularity
    except:
        return "", 0, 0

# register the UDF and specify return types for each field
# skip explicitly defining the StructType in the UDF, and simply return a Python dictionary or tuple. However, it is a good practice to define the return type for UDFs in PySpark, as it makes the code more efficient and helps avoid type-related errors. Simply use: udf(get_artist_info)
@udf(returnType=StructType([
    StructField("genres", StringType(), True),
    StructField("followers", IntegerType(), True),
    StructField("popularity", IntegerType(), True)
]))

def artist_info_udf(artist_id):
    return get_artist_info(artist_id)
    

def process_artists(df):
    df_items_exploded = df.select(explode(col("items")).alias("item"))
    
    artists_df_exploded = df_items_exploded.select(
        explode(col("item.track.artists")).alias("artist"))
    
    artists_df = artists_df_exploded.select(
        col("artist.id").alias("artist_id"),
        col("artist.name").alias("artist_name"),
        col("artist.external_urls.spotify").alias("url")
    ).drop_duplicates(["artist_id"])
    
    artists_df = artists_df.withColumn("artist_info", artist_info_udf(col("artist_id")))
    
    artists_df = artists_df.select(
        col("artist_id"),
        col("artist_name"),
        col("url"),
        col("artist_info.genres").alias("artist_genres"),
        col("artist_info.followers").alias("artist_followers"),
        col("artist_info.popularity").alias("popularity")
    )
    
    return artists_df
    
    
def process_songs(df):
    df_items_exploded = df.select(explode(col("items")).alias("item"))
    
    df_songs_exploded = df_items_exploded.withColumn(
        "artist_id",
        explode(col("item.track.artists.id"))
    )

    # Create a new DataFrame that has track_id and artist_id
    df_songs_artist_exploded = df_songs_exploded.select(
        col("item.track.id").alias("song_id"),
        col("artist_id")
    )

    df_song_artist = df_songs_artist_exploded.groupBy("song_id") \
            .agg(concat_ws(", ", collect_list("artist_id")).alias("artist_ids"))
    
    df_songs = df_items_exploded.select(
        col("item.track.id").alias("song_id"),
        col("item.track.name").alias("song_name"),
        col("item.track.duration_ms").alias("duration_ms"),
        col("item.track.popularity").alias("popularity"),
        col("item.track.external_urls.spotify").alias("url"),
        col("item.added_at").alias("song_added"),
        col("item.track.album.id").alias("album_id")
    ).drop_duplicates(["song_id"])
    
    df_songs = df_songs.withColumn("song_added", to_date(col("song_added")))
    final_df = df_songs.join(df_song_artist, on="song_id", how="inner")
    
    return final_df
 
album_df = process_albums(spotify_df)
artist_df = process_artists(spotify_df)
song_df = process_songs(spotify_df)


def write_to_s3(df, path_suffix, format_type="csv"):
    dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")
    
    glueContext.write_dynamic_frame.from_options(
        frame = dynamic_frame,
        connection_type = "s3",
        connection_options = {"path": f"s3://spotify-etl-project-tqx/transformed_data/{path_suffix}/"}, 
        format = format_type
    )
write_to_s3(album_df, "album_data/album_transformed_{}".format(datetime.now().strftime("%Y%m%d")), "csv")
write_to_s3(artist_df, "artist_data/artist_transformed_{}".format(datetime.now().strftime("%Y%m%d")), "csv")
write_to_s3(song_df, "song_data/song_transformed_{}".format(datetime.now().strftime("%Y%m%d")), "csv")
job.commit()