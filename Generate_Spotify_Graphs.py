# Databricks notebook source
# MAGIC %md
# MAGIC # Graph Example - Startups East Spotify Graph

# COMMAND ----------

# MAGIC %pip install spotipy

# COMMAND ----------

dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "reset_all_data")
dbutils.widgets.text("userIds", "ben.doan4366,abafzal,1248411767,121025019,chrissteingass,thecarlhall")

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
from functools import reduce

from graphframes import *

# COMMAND ----------

user_ids = ["ben.doan4366","abafzal","1248411767","121025019","chrissteingass","thecarlhall"]

# COMMAND ----------

albums = spark.read.table("doan_demo_catalog.doan_demo_database.spotify_albums")
tracks = spark.read.table("doan_demo_catalog.doan_demo_database.spotify_tracks")
user_playlists = spark.read.table("doan_demo_catalog.doan_demo_database.spotify_graph_users")

# COMMAND ----------

display(user_playlists)

# COMMAND ----------

vertex_table_schema = StructType(
    [
        StructField("id", StringType(), True),
        StructField("vertex_type", StringType(), True),
        StructField("vertex_properties", StringType(), True)
    ]
  )

edges_table_schema = StructType(
    [
        StructField("src", StringType(), True),
        StructField("dst", StringType(), True),
        StructField("relationship", StringType(), True),
    ]
  )

# COMMAND ----------

vertex_definitions = [
    {
        "vertex_id": "albumId",
        "vertex_type": "album",
        "vertex_properties": ["albumName"],
        "source": albums,
        "edge_relationships": "albums",
    },
    {
        "vertex_id": "trackId",
        "vertex_type": "track",
        "vertex_properties": ["trackName"],
        "source": tracks,
        "edge_relationships": "tracks",
    },
    {
        "vertex_id": "artistId",
        "vertex_type": "artist",
        "vertex_properties": ["artistName"],
        "source": tracks,
        "edge_relationships": "tracks",
    },
    {   
        "vertex_id": "playlist_id",
        "vertex_type": "playlist",
        "vertex_properties": ["playlist_name"],
        "source": user_playlists,
        "edge_relationships": "user_playlists",
    },
    {   
        "vertex_id": "playlist_owner",
        "vertex_type": "user",
        "vertex_properties": ["playlist_id"],
        "source": user_playlists,
        "edge_relationships": "user_playlists",
    },
]

# COMMAND ----------

edge_relationships = {
    "albums": [
        {
            "src": "artistId",
            "dst": "albumId",
            "relationship": "created",
        }
    ],
    "tracks": [
        {
            "src": "trackId", 
            "dst": "albumId", 
            "relationship": "listed_on"
        },
        {
            "src": "artistId",
            "dst": "trackId",
            "relationship": "featured_on",
        },
        {
            "src": "trackId",
            "dst": "playlistId",
            "relationship": "added_to",
        },
    ],
    "user_playlists": [
        {
            "src": "playlist_owner",
            "dst": "playlist_id",
            "relationship": "follows",
        }
    ]
}



# COMMAND ----------

from typing import List

def generate_vertices(df, vertex_id:str, vertex_type:str, vertex_properties:List[str]):
  
  vertex_df = df.withColumnRenamed(vertex_id, "id")\
                .withColumn("vertexType", lit(vertex_type))\
                .select("id","vertexType", *vertex_properties)\
                .withColumnRenamed(*vertex_properties, "properties")
  
  return vertex_df

  
def generate_edges(df, edge_definitions: List[dict]):
  
  edgesDF = spark.createDataFrame([], edges_table_schema)
  
  for d in edge_definitions:
    
    tempEdgesDF = df.select(d["src"], d["dst"])
    tempEdgesDF = tempEdgesDF.withColumnRenamed(d["src"], "src")\
                  .withColumnRenamed(d["dst"], "dst")\
                  .withColumn("relationship", lit(d["relationship"]))
    
    edgesDF = edgesDF.unionAll(tempEdgesDF)
    
  return edgesDF


def create_graph_dataframes(vertex_definitions, edge_relationships):
  
  verticesDF = spark.createDataFrame([], vertex_table_schema)
  edgesDF = spark.createDataFrame([], edges_table_schema)
  
  for vertex in vertex_definitions:
    
    tempVertexDF = generate_vertices(vertex["source"],vertex["vertex_id"],vertex["vertex_type"], vertex["vertex_properties"]) 
    verticesDF = verticesDF.unionAll(tempVertexDF)
    
    tempEdgeDF = generate_edges(vertex["source"], edge_relationships[vertex["edge_relationships"]])
    edgesDF = edgesDF.unionAll(tempEdgeDF)
    
  verticesDF = verticesDF.distinct()
  
  return verticesDF, edgesDF
    
    
    

# COMMAND ----------

spotifyVerticesDF, spotifyEdgesDF = create_graph_dataframes(vertex_definitions, edge_relationships)

# COMMAND ----------

display(spotifyVerticesDF)

# COMMAND ----------

display(spotifyEdgesDF)

# COMMAND ----------

spotifyVerticesDF.write.format("delta").mode("overwrite").saveAsTable("doan_demo_catalog.doan_demo_database.spotify_graphframes_vertices")
spotifyEdgesDF.write.format("delta").mode("overwrite").saveAsTable("doan_demo_catalog.doan_demo_database.spotify_graphframes_edges")

# COMMAND ----------

spotifyGraph = GraphFrame(spotifyVerticesDF, spotifyEdgesDF)

spotifyVerticesDF.cache()
spotifyEdgesDF.cache()

# COMMAND ----------

outDeg = spotifyGraph.outDegrees
inDeg = spotifyGraph.inDegrees

# COMMAND ----------

# MAGIC %md
# MAGIC ## Roughly Measure Influence by Calculating Out Degrees
# MAGIC
# MAGIC One way to measure influence is to measure the out-degrees of a given vertex. In other words, how many edges point out from a given vertex?
# MAGIC <img src="https://pepvids.sgp1.cdn.digitaloceanspaces.com/articles/introduction_to_graphs_and_its_representation/introduction_to_graphs_and_its_represtation_10.png"
# MAGIC      alt="Markdown Monster icon"
# MAGIC      width=500px />

# COMMAND ----------

artist_out_degrees = outDeg.join(albums.select("artistId","artistName"), outDeg.id == albums.artistId, "inner")

display(artist_out_degrees.orderBy("outDegree", desc=True).distinct())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Strongly Connected Components
# MAGIC
# MAGIC A strongly connected component is the portion of a directed graph in which there is a path from each vertex to another vertex. It is applicable only on a directed graph.
# MAGIC
# MAGIC <img src="https://cdn.programiz.com/sites/tutorial2program/files/scc-strongly-connected-components.png"
# MAGIC      alt="Markdown Monster icon"
# MAGIC      width=500px />
# MAGIC
# MAGIC
# MAGIC **Applications**
# MAGIC * **Threat Modeling:** resources that are SCCs are often in the same threat vector. If one is compromised other SCCs are often the most at-risk
# MAGIC * **Social Media Analysis:** People who are often strongly connected often have common likes and habits, which and be used to boost the performance of recommendation engines

# COMMAND ----------


