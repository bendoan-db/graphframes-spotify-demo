# Databricks notebook source
tracks = spark.read.table("doan_demo_database.spotify_graph_tracks")
print("hello!")

# COMMAND ----------

[tracks.columns]

# COMMAND ----------

display(tracks.select("trackName",'artistName',
  'uri',
  'artistId',
  'type',
  'href',
  'playlistId',
  'duration_ms',
  'trackId',
  'albumId').distinct())

# COMMAND ----------


