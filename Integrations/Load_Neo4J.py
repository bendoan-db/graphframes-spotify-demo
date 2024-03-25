# Databricks notebook source
# MAGIC %md
# MAGIC install maven package: org.neo4j:neo4j-connector-apache-spark_2.12:5.3.0_for_spark_3

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

users = spark.read.table("doan_demo_catalog.doan_demo_database.spotify_graph_users")
albums = spark.read.table("doan_demo_catalog.doan_demo_database.spotify_albums")
tracks = spark.read.table("doan_demo_catalog.doan_demo_database.spotify_tracks")

# COMMAND ----------

edges = spark.read.table("doan_demo_catalog.doan_demo_database.spotify_graphframes_edges")

# COMMAND ----------

users_v = users.select("playlist_owner").dropDuplicates(["playlist_owner"])
playlists_v = users.select("playlist_id", "playlist_name").dropDuplicates(["playlist_id"])
tracks_v = tracks.select("trackId", "trackName", "duration_ms").dropDuplicates(["trackId"])
artist_v = tracks.select("artistId", "artistName").dropDuplicates(["artistId"])
albums_v = albums.select("albumId", "albumName", "uri").dropDuplicates(["albumId"])

# COMMAND ----------

password = "xxx"
url = "neo4j+s://4d2c59c5.databases.neo4j.io"

# COMMAND ----------

(users_v.write
  .format("org.neo4j.spark.DataSource")
  .mode("overwrite")
  .option("authentication.basic.username", "neo4j")
  .option("authentication.basic.password", password)
  .option("url", url)
  .option("labels", ":user")
  .option("node.keys", "playlist_owner")
  .save())

# COMMAND ----------

(playlists_v.write
  .format("org.neo4j.spark.DataSource")
  .mode("overwrite")
  .option("authentication.basic.username", "neo4j")
  .option("authentication.basic.password", password)
  .option("url", url)
  .option("labels", ":playlist")
  .option("node.keys", "playlist_id")
  .save())

(tracks_v.write
  .format("org.neo4j.spark.DataSource")
  .mode("overwrite")
  .option("authentication.basic.username", "neo4j")
  .option("authentication.basic.password", password)
  .option("url", url)
  .option("labels", ":track")
  .option("node.keys", "trackId")
  .save())

(artist_v.write
  .format("org.neo4j.spark.DataSource")
  .mode("overwrite")
  .option("authentication.basic.username", "neo4j")
  .option("authentication.basic.password", password)
  .option("url", url)
  .option("labels", ":artist")
  .option("node.keys", "artistId")
  .save())


(albums_v.write
  .format("org.neo4j.spark.DataSource")
  .mode("overwrite")
  .option("authentication.basic.username", "neo4j")
  .option("authentication.basic.password", password)
  .option("url", url)
  .option("labels", ":album")
  .option("node.keys", "albumId")
  .save())

# COMMAND ----------

follows = edges.filter(col("relationship") == "follows").withColumnRenamed("src","playlist_owner").withColumnRenamed("dst","playlist_id")
featured_on = edges.filter(col("relationship") == "featured_on").withColumnRenamed("src","artistId").withColumnRenamed("dst","trackId")
created = edges.filter(col("relationship") == "created").withColumnRenamed("src","artistId").withColumnRenamed("dst","albumId")
added_to = edges.filter(col("relationship") == "added_to").withColumnRenamed("src","trackId").withColumnRenamed("dst","playlist_id")
listed_on = edges.filter(col("relationship") == "listed_on").withColumnRenamed("src","trackId").withColumnRenamed("dst","albumId")

# COMMAND ----------

featured_on.count()

# COMMAND ----------

(follows.coalesce(1)
    .write
    .format("org.neo4j.spark.DataSource")
    .option("authentication.basic.username", "neo4j")
    .option("authentication.basic.password", password)
    .option("url", url)
    .mode("overwrite")
    .option("relationship", "FOLLOWS")
    .option("relationship.save.strategy", "keys")
    .option("relationship.source.labels", ":user")
    .option("relationship.source.save.mode", "overwrite")
    .option("relationship.source.node.keys", "playlist_owner")
    .option("relationship.target.labels", ":playlist")
    .option("relationship.target.node.keys", "playlist_id")
    .option("relationship.target.save.mode", "overwrite")
    .save())

# COMMAND ----------

(featured_on.coalesce(1).write
    .format("org.neo4j.spark.DataSource")
    .option("authentication.basic.username", "neo4j")
    .option("authentication.basic.password", password)
    .option("url", url)
    .mode("overwrite")
    .option("relationship", "FEATURED_ON")
    .option("relationship.save.strategy", "keys")
    .option("relationship.source.labels", ":artist")
    .option("relationship.source.save.mode", "overwrite")
    .option("relationship.source.node.keys", "artistId")
    .option("relationship.target.labels", ":track")
    .option("relationship.target.node.keys", "trackId")
    .option("relationship.target.save.mode", "overwrite")
    .save())

# COMMAND ----------

(created.coalesce(1).write
    .format("org.neo4j.spark.DataSource")
    .option("authentication.basic.username", "neo4j")
    .option("authentication.basic.password", password)
    .option("url", url)
    .mode("overwrite")
    .option("relationship", "CREATED")
    .option("relationship.save.strategy", "keys")
    .option("relationship.source.labels", ":artist")
    .option("relationship.source.save.mode", "overwrite")
    .option("relationship.source.node.keys", "artistId")
    .option("relationship.target.labels", ":album")
    .option("relationship.target.node.keys", "albumId")
    .option("relationship.target.save.mode", "overwrite")
    .save())

# COMMAND ----------

(added_to.coalesce(1).write
    .format("org.neo4j.spark.DataSource")
    .option("authentication.basic.username", "neo4j")
    .option("authentication.basic.password", password)
    .option("url", url)
    .mode("overwrite")
    .option("relationship", "ADDED_TO")
    .option("relationship.save.strategy", "keys")
    .option("relationship.source.labels", ":track")
    .option("relationship.source.save.mode", "overwrite")
    .option("relationship.source.node.keys", "trackId")
    .option("relationship.target.labels", ":playlist")
    .option("relationship.target.node.keys", "playlist_id")
    .option("relationship.target.save.mode", "overwrite")
    .save())

# COMMAND ----------

(listed_on.coalesce(1).write
    .format("org.neo4j.spark.DataSource")
    .option("authentication.basic.username", "neo4j")
    .option("authentication.basic.password", password)
    .option("url", url)
    .mode("overwrite")
    .option("relationship", "ADDED_TO")
    .option("relationship.save.strategy", "keys")
    .option("relationship.source.labels", ":track")
    .option("relationship.source.save.mode", "overwrite")
    .option("relationship.source.node.keys", "trackId")
    .option("relationship.target.labels", ":album")
    .option("relationship.target.node.keys", "albumId")
    .option("relationship.target.save.mode", "overwrite")
    .save())

# COMMAND ----------

display(follows)

# COMMAND ----------


