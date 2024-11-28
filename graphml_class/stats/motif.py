#!/usr/bin/env pyspark --packages graphframes:graphframes:0.8.3-spark3.5-s_2.12 --driver-memory 16g --executor-memory 8g

import os

import pandas as pd
import pyspark.sql.functions as F
from graphframes import GraphFrame
from pyspark import SparkContext
from pyspark.sql import DataFrame, SparkSession

# This is actually already set in Docker, just reminding you Java is needed
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"

# Setup PySpark to use the GraphFrames jar package from maven central
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages graphframes:graphframes:0.8.3-spark3.5-s_2.12 pyspark-shell "
    "--driver-memory 4g pyspark-shell "
    "--executor-memory 4g pyspark-shell "
    "--driver-java-options='-Xmx4g -Xms4g' "
)

# Show all the rows in a pd.DataFrame
pd.set_option("display.max_columns", None)


#
# Clean up any previous SparkSession so we can create a new one with > 1g RAM
#
if "spark" in locals() or "spark" in globals():
    try:
        spark.stop()  # type: ignore
        del sc  # type: ignore
        del spark  # type: ignore
        print("Stopped the existing SparkSession to start one with higher memory.")
    except NameError:
        print("No existing SparkSession to stop.")


#
# Initialize a SparkSession. You can configre SparkSession via: .config("spark.some.config.option", "some-value")
#
spark: SparkSession = (
    SparkSession.builder.appName("Stack Overflow Motif Analysis")
    # Lets the Id:(Stack Overflow int) and id:(GraphFrames ULID) coexist
    .config("spark.sql.caseSensitive", True)
    # Single node mode - 128GB machine
    .config("spark.driver.memory", "16g")
    .config("spark.executor.memory", "8g")
    .getOrCreate()
)
sc: SparkContext = spark.sparkContext


#
# Store the nodes to disk, reload and cache
#
NODES_PATH: str = "data/stats.meta.stackexchange.com/Nodes.parquet"
nodes_df: DataFrame = spark.read.parquet(NODES_PATH)

# Helps performance of GraphFrames' algorithms
nodes_df = nodes_df.cache()

# What kind of nodes we do we have to work with?
nodes_df.select("id", "Type").groupBy("Type").count().show()

EDGES_PATH: str = "data/stats.meta.stackexchange.com/Edges.parquet"
relationships_df: DataFrame = spark.read.parquet(EDGES_PATH)

# What kind of edges do we have to work with?
relationships_df.select("src", "dst", "relationship").groupBy("relationship").count().show()

# Helps performance of GraphFrames' algorithms
relationships_df: DataFrame = relationships_df.cache()


#
# Create the GraphFrame
#
g = GraphFrame(nodes_df, relationships_df)

g.vertices.show()
g.edges.show()

# Test out Connected Components - fun!
sc.setCheckpointDir("/tmp/spark-checkpoints")

# This is the top used algorithm in GraphFrames - really useful for big data entity resolution!
components = g.connectedComponents()
components.select("id", "component").groupBy("component").count().sort(F.desc("count")).show()

# Shows (User)-[VotedFor]->(Post)--(Answers)->(Post)
paths = g.find("(a)-[e]->(b); (b)-[e2]->(c); (c)-[e3]->(a)")
paths.select("a.Type", "e.*", "b.Type", "e2.*", "c.Type").show()

# Shows two matches:
# - (Post)-[Answers]->(Post)<--[Posted]-(User)
# - (Post)-[Answers]->(Post)<--[VotedFor]-(User)
paths = g.find("(a)-[e]->(b); (c)-[e2]->(a)")
paths.select("a.Type", "e.*", "b.Type", "e2.*", "c.Type").show()

# Figure out how often questions are answered and the question poster votes for the answer. Neat!
# Shows (User A)-[Posted]->(Post)<-[Answers]-(Post)<-[VotedFor]-(User)
paths = g.find("(a)-[e]->(b); (c)-[e2]->(b); (d)-[e3]->(c)")

# If the node types are right...
paths = paths.filter(F.col("a.Type") == "User")
paths = paths.filter(F.col("b.Type") == "Post")
paths = paths.filter(F.col("c.Type") == "Post")
paths = paths.filter(F.col("d.Type") == "User")

# If the edge types are right...
# paths = paths.filter(F.col("e.relationship") == "Posted")
# paths = paths.filter(F.col("e2.relationship") == "Answers")
paths = paths.filter(F.col("e3.relationship") == "VotedFor")

paths.select(
    "a.Type",
    "e.relationship",
    "b.Type",
    "e2.relationship",
    "c.Type",
    "e3.relationship",
    "d.Type",
).show()
