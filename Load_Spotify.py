# Databricks notebook source
# MAGIC %md
# MAGIC # Graph Example - Startups East Spotify Graph

# COMMAND ----------

# MAGIC %pip install spotipy

# COMMAND ----------

import os
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from spotipy.oauth2 import SpotifyClientCredentials

import pandas as pd
import json

import pyspark
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

from graphframes import *

# COMMAND ----------

client_id = "048069deb13641da84c999f457c3963a"
client_secret = "3b8ab7aa5f144e69bdc49663297017db"

# COMMAND ----------

auth_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
sp = spotipy.Spotify(auth_manager=auth_manager)

# COMMAND ----------

user_ids = ["ben.doan4366","abafzal","1248411767","121025019","chrissteingass","thecarlhall"]

# COMMAND ----------

user_playlists = sp.user_playlists("ben.doan4366")
user_playlists

# COMMAND ----------

album_artist_sub_schema = StructType(
    [
        StructField("name", StringType(), True),
        StructField("id", StringType(), True),
        StructField("href", StringType(), True),
        StructField("type", StringType(), True),
        StructField("uri", StringType(), True),
        StructField("external_urls", StringType(), True),
    ]
)
album_item_schema = StructType(
    [
        StructField("name", StringType(), True),
        StructField("uri", StringType(), True),
        StructField("id", StringType(), True),
        StructField("artists", ArrayType(album_artist_sub_schema, True), True),
        StructField("href", StringType(), True),
        StructField("images", StringType(), True),
        StructField("external_urls", StringType(), True),
        StructField("available_markets", StringType(), True),
        StructField("release_day_precision", StringType(), True),
    ]
)
album_full_schema = StructType([StructField("album", album_item_schema, True)])

artist_item_schema = StructType(
    [
        StructField("name", StringType(), True),
        StructField("uri", StringType(), True),
        StructField("id", StringType(), True),
        StructField("type", StringType(), True),
        StructField("href", StringType(), True),
    ]
)
artist_full_schema = StructType(
    [StructField("artists", ArrayType(artist_item_schema, True), True)]
)

playlist_tracks_schema = StructType(
    [
        StructField("album", album_item_schema, True),
        StructField("artists", ArrayType(artist_item_schema, True), True),
        StructField("available_markets", ArrayType(StringType(), True), True),
        StructField("disc_number", LongType(), True),
        StructField("duration_ms", LongType(), True),
        StructField("episode", BooleanType(), True),
        StructField("explicit", BooleanType(), True),
        StructField("external_ids", MapType(StringType(), StringType(), True), True),
        StructField("external_urls", MapType(StringType(), StringType(), True), True),
        StructField("href", StringType(), True),
        StructField("id", StringType(), True),
        StructField("is_local", BooleanType(), True),
        StructField("name", StringType(), True),
        StructField("popularity", LongType(), True),
        StructField("preview_url", StringType(), True),
        StructField("track", BooleanType(), True),
        StructField("track_number", LongType(), True),
        StructField("type", StringType(), True),
        StructField("uri", StringType(), True),
    ]
)

albums_final_df_schema = StructType(
    [
        StructField("albumName", StringType(), True),
        StructField("albumId", StringType(), True),
        StructField("artistName", StringType(), True),
        StructField("artistId", StringType(), True),
        StructField("href", StringType(), True),
        StructField("type", StringType(), True),
        StructField("uri", StringType(), True),
        StructField("external_urls", StringType(), True),
        StructField("playlistId", StringType(), False),
    ]
)

tracks_final_df_schema = StructType(
    [
        StructField("artistName", StringType(), True),
        StructField("uri", StringType(), True),
        StructField("artistId", StringType(), True),
        StructField("type", StringType(), True),
        StructField("href", StringType(), True),
        StructField("playlistId", StringType(), True),
        StructField("trackName", StringType(), True),
        StructField("duration_ms", LongType(), True),
        StructField("trackId", StringType(), True),
        StructField("albumId", StringType(), True),
    ]
)

user_playlist_schema = StructType(
    [
        StructField("playlist_id", StringType(), True),
        StructField("playlist_name", StringType(), True),
        StructField("playlist_owner", StringType(), True)
    ]
)

# COMMAND ----------

albums_final = spark.createDataFrame([],albums_final_df_schema)
tracks_final = spark.createDataFrame([],tracks_final_df_schema)

for user in user_ids:
  user_playlists = sp.user_playlists(user)
  user_playlist_ids = [playlist['id'] for playlist in user_playlists["items"]][:50]
  
  for playlist_id in user_playlist_ids:
    playlist_tracks = sp.playlist_tracks(playlist_id)
    playlist_track_items = [item['track'] for item in playlist_tracks['items']]
    
    playlist_tracks_df = spark.createDataFrame(playlist_track_items,playlist_tracks_schema).withColumn("playlistId", lit(playlist_id))
    
    albums = (playlist_tracks_df
            .select("album.*", "playlistId")
            .select(col("name").alias("albumName"), col("id").alias("albumId"), explode("artists"), "playlistId")
            .select("albumName", "albumId", "col.*", "playlistId")
            .withColumnRenamed("name", "artistName")
            .withColumnRenamed("id", "artistId")
         )

    tracks = (playlist_tracks_df
                .select(explode("artists"), "playlistId", col("name").alias("trackName"), "duration_ms", col("id").alias("trackId"), col("album.id").alias("albumId"))
                .select("col.*","playlistId", "trackName", "duration_ms", "trackId", "albumId")
                .withColumnRenamed("id", "artistId")
                .withColumnRenamed("name", "artistName")
             )
    
    albums_final = albums_final.unionAll(albums)
    tracks_final = tracks_final.unionAll(tracks)

# COMMAND ----------

display(playlist_tracks_df)

# COMMAND ----------

playlist_tracks = sp.playlist_tracks("0C4IFYyjcSJ2nsgqsHWhxC")
playlist_track_items = [item['track'] for item in playlist_tracks['items']]

playlist_tracks_df = spark.createDataFrame(playlist_track_items,playlist_tracks_schema).withColumn("playlistId", lit(playlist_id))

# COMMAND ----------

playlist_tracks_df.select("name","artists","album","playlistId").schema

# COMMAND ----------

StructType(
    [
        StructField("trackName", StringType(), True),
        StructField("artists",
            ArrayType(
                StructType(
                    [
                        StructField("name", StringType(), True),
                        StructField("uri", StringType(), True),
                        StructField("id", StringType(), True),
                    ]),True,),True,
        ),
        StructField(
            "album",
            StructType(
                [
                    StructField("name", StringType(), True),
                    StructField("uri", StringType(), True),
                    StructField("id", StringType(), True),
                    StructField(
                        "artists",
                        ArrayType(
                            StructType(
                                [
                                    StructField("name", StringType(), True),
                                    StructField("id", StringType(), True),
                                    StructField("href", StringType(), True),
                                    StructField("type", StringType(), True),
                                    StructField("uri", StringType(), True),
                                    StructField("external_urls", StringType(), True),
                                ]
                            ),
                            True,
                        ),
                        True,
                    )
                ]
            ),
            True,
        ),
        StructField("playlistId", StringType(), False),
    ]
)

# COMMAND ----------

playlist_owners_df = spark.createDataFrame([],user_playlist_schema)

for user in user_ids:
  user_playlists = sp.user_playlists(user)

  playlist_owner_dict = [{"playlist_id":item["id"],"playlist_name":item["name"]} for item in user_playlists["items"]]
  tempDF = spark.createDataFrame(playlist_owner_dict, user_playlist_schema).withColumn("playlist_owner", lit(user))

  playlist_owners_df = playlist_owners_df.unionAll(tempDF)

# COMMAND ----------

playlist_owners_df_final = (playlist_owners_df
  .withColumn("playlist_owner", regexp_replace("playlist_owner", "1248411767", "Rohith Chintalapally"))
  .withColumn("playlist_owner", regexp_replace("playlist_owner", "121025019", "Artem Yevtushenko")))

# COMMAND ----------

playlist_owners_df_final.write.format("delta").mode("overwrite").saveAsTable("doan_demo_database.spotify_graph_users")
albums_final.write.format("delta").mode("overwrite").saveAsTable("doan_demo_database.spotify_graph_albums")
tracks_final.write.format("delta").mode("overwrite").saveAsTable("doan_demo_database.spotify_graph_tracks")
