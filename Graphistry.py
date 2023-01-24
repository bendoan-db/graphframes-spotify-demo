# Databricks notebook source
# MAGIC %run ./Load_Spotify

# COMMAND ----------

!pip install graphistry

# COMMAND ----------

spark.conf.set("spark.sql.execution.arrow.enabled", "true")

# COMMAND ----------

password = "P1usUltr@!"

# COMMAND ----------

import graphistry  # if not yet available, install and/or restart Python kernel using the above

# To specify Graphistry account & server, use:
graphistry.register(api=3, username='bendoan-db', password=password, protocol='https', server='hub.graphistry.com')
# For more options, see https://github.com/graphistry/pygraphistry#configure

graphistry.__version__

# COMMAND ----------

vertices = spark.read.table("doan_demo_database.graphframe_vertices")
edges = spark.read.table("doan_demo_database.graphframe_edges")

# COMMAND ----------

import pyspark.sql.functions as F
color_data = [["user", 15408149], ["playlist", 2463422], ["track", 376309], ["artist", 14680064], ["album", 22222]]
color_columns = ["vtype", "color_code"]

color_mappings = spark.createDataFrame(color_data, color_columns)

# COMMAND ----------

gv = vertices.join(color_mappings, vertices.vertex_type == color_mappings.vtype).select("id", "vertex_properties", "color_code")
display(gv)

# COMMAND ----------

p = (graphistry
    .bind(point_title='vertex_properties')
    .nodes(gv, 'id')
    .bind(edge_title='relationship')
    .edges(edges, 'src', 'dst')
    .settings(url_params={'strongGravity': 'true'})
    .plot()
)

# COMMAND ----------

p

# COMMAND ----------


