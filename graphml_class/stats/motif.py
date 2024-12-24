#!/usr/bin/env pyspark --packages graphframes:graphframes:0.8.3-spark3.5-s_2.12 --driver-memory 16g --executor-memory 8g

import os

import pandas as pd
import pyspark.sql.functions as F
from graphframes import GraphFrame
from pyspark import SparkContext
from pyspark.sql import DataFrame, SparkSession


def three_edge_count(paths: DataFrame) -> DataFrame:
    """three_edge_count View the counts of the different types of 3-node graphlets in the graph.

    Parameters
    ----------
    paths : pyspark.sql.DataFrame
        A DataFrame of 3-paths in the graph.

    Returns
    -------
    DataFrame
        A DataFrame of the counts of the different types of 3-node graphlets in the graph.
    """
    graphlet_type_df = paths.select(
        F.col("a.Type").alias("A_Type"),
        F.col("e.relationship").alias("E_relationship"),
        F.col("b.Type").alias("B_Type"),
        F.col("e2.relationship").alias("E2_relationship"),
        F.col("c.Type").alias("C_Type"),
        F.col("e3.relationship").alias("E3_relationship"),
    )
    graphlet_count_df = (
        graphlet_type_df.groupby(
            "A_Type", "E_relationship", "B_Type", "E2_relationship", "C_Type", "E3_relationship"
        )
        .count()
        .orderBy(F.col("count").desc())
    )
    return graphlet_count_df


def four_edge_count(paths: DataFrame) -> DataFrame:
    """four_edge_count View the counts of the different types of 4-node graphlets in the graph.

    Parameters
    ----------
    paths : DataFrame
        A DataFrame of 4-paths in the graph.

    Returns
    -------
    DataFrame
        A DataFrame of the counts of the different types of 4-node graphlets in the graph.
    """

    graphlet_type_df = paths.select(
        F.col("a.Type").alias("A_Type"),
        F.col("e.relationship").alias("E_relationship"),
        F.col("b.Type").alias("B_Type"),
        F.col("e2.relationship").alias("E2_relationship"),
        F.col("c.Type").alias("C_Type"),
        F.col("e3.relationship").alias("E3_relationship"),
        F.col("d.Type").alias("D_Type"),
        F.col("e4.relationship").alias("E4_relationship"),
    )
    graphlet_count_df = (
        graphlet_type_df.groupby(
            "A_Type",
            "E_relationship",
            "B_Type",
            "E2_relationship",
            "C_Type",
            "E3_relationship",
            "D_Type",
            "E4_relationship",
        )
        .count()
        .orderBy(F.col("count").desc())
    )
    return graphlet_count_df


def add_degree(g: GraphFrame) -> GraphFrame:
    """add_degree compute the degree, adding it as a property of the nodes in the GraphFrame.

    Parameters
    ----------
    g : GraphFrame
        Any valid GraphFrame

    Returns
    -------
    GraphFrame
        Same GraphFrame with a 'degree' property added
    """
    degree_vertices: DataFrame = g.vertices.join(g.degrees, on="id")
    return GraphFrame(degree_vertices, g.edges)


def add_type_degree(g: GraphFrame) -> GraphFrame:
    """add_type_degree add a map property to the vertices with the degree by each type of relationship.

    Parameters
    ----------
    g : GraphFrame
        Any valid GraphFrame

    Returns
    -------
    GraphFrame
        A GraphFrame with a map[type:degree] 'type_degree' field added to the vertices
    """
    type_degree: DataFrame = (
        g.edges.select(F.col("src").alias("id"), "relationship")
        .filter(F.col("id").isNotNull())
        .groupby("id", "relationship")
        .count()
    )
    type_degree = type_degree.withColumn("type_degree", F.create_map(type_degree.columns))
    type_degree = type_degree.select("src", "type_degree")
    return g.vertices.join(type_degree, on="src")


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

# Add the degree to use as a property in the motifs
g = add_degree(g).cache()

g.vertices.show()
g.edges.show()

# Test out Connected Components - fun!
sc.setCheckpointDir("/tmp/spark-checkpoints")

# This is the top used algorithm in GraphFrames - really useful for big data entity resolution!
components = g.connectedComponents()
components.select("id", "component").groupBy("component").count().sort(F.desc("count")).show()

# G4: Continuous Triangles
paths = g.find("(a)-[e]->(b); (b)-[e2]->(c); (c)-[e3]->(a)")
three_edge_count(paths).show()

# G5: Divergent Triangles
paths = g.find("(a)-[e]->(b); (a)-[e2]->(c); (c)-[e3]->(b)")
three_edge_count(paths).show()

# G6: Continuous Path
paths = g.find("(a)-[e]->(b); (b)-[e2]->(c); (c)-[e3]->(d)")
three_edge_count(paths).show()

# G6: Continuous Path Votes with VoteTypes
graphlet_type_df = paths.select(
    F.col("a.Type").alias("A_Type"),
    F.col("a.VoteType").alias("A_VoteType"),
    F.col("e.relationship").alias("E_relationship"),
    F.col("b.Type").alias("B_Type"),
    F.col("e2.relationship").alias("E2_relationship"),
    F.col("c.Type").alias("C_Type"),
    F.col("e3.relationship").alias("E3_relationship"),
)
graphlet_count_df = (
    graphlet_type_df.groupby(
        "A_Type",
        "A_VoteType",
        "E_relationship",
        "B_Type",
        "E2_relationship",
        "C_Type",
        "E3_relationship",
    )
    .count()
    .filter(F.col("A_Type") == "Vote")
    .orderBy(F.col("count").desc())
)
graphlet_count_df.show()

# +------+----------+--------------+------+---------------+------+---------------+-----+
# |A_Type|A_VoteType|E_relationship|B_Type|E2_relationship|C_Type|E3_relationship|count|
# +------+----------+--------------+------+---------------+------+---------------+-----+
# |  Vote|    UpVote|       CastFor|  Post|        Answers|  Post|          Links|27197|
# |  Vote|    UpVote|       CastFor|  Post|          Links|  Post|          Links|22947|
# |  Vote|  DownVote|       CastFor|  Post|        Answers|  Post|          Links| 2129|
# |  Vote|  DownVote|       CastFor|  Post|          Links|  Post|          Links| 1503|
# |  Vote|   Unknown|       CastFor|  Post|        Answers|  Post|          Links|  523|
# |  Vote|   Unknown|       CastFor|  Post|          Links|  Post|          Links|  165|
# |  Vote|      Spam|       CastFor|  Post|          Links|  Post|          Links|   18|
# |  Vote|    UpVote|       CastFor|  Post|          Links|  Post|        Answers|   17|
# |  Vote|      Spam|       CastFor|  Post|        Answers|  Post|          Links|   16|
# |  Vote|Undeletion|       CastFor|  Post|        Answers|  Post|          Links|    1|
# |  Vote|   Unknown|       CastFor|  Post|          Links|  Post|        Answers|    1|
# |  Vote|    Reopen|       CastFor|  Post|          Links|  Post|          Links|    1|
# +------+----------+--------------+------+---------------+------+---------------+-----+

# G10: Convergent Triangle
paths = g.find("(a)-[e]->(b); (c)-[e2]->(a); (d)-[e3]->(a)")
three_edge_count(paths).show()

# G10: Convergent Triangle with VoteTypes
paths = g.find("(a)-[e]->(b); (c)-[e2]->(a); (d)-[e3]->(a)")
graphlet_type_df = paths.select(
    F.col("a.Type").alias("A_Type"),
    F.col("e.relationship").alias("E_relationship"),
    F.col("b.Type").alias("B_Type"),
    F.col("e2.relationship").alias("E2_relationship"),
    F.col("c.Type").alias("C_Type"),
    F.col("c.VoteType").alias("C_VoteType"),
    F.col("e3.relationship").alias("E3_relationship"),
    F.col("d.Type").alias("D_Type"),
    F.col("d.VoteType").alias("D_VoteType"),
)
graphlet_count_df = (
    graphlet_type_df.groupby(
        "A_Type",
        "E_relationship",
        "B_Type",
        "E2_relationship",
        "C_Type",
        "C_VoteType",
        "E3_relationship",
        "D_Type",
        "D_VoteType",
    )
    .count()
    .filter(F.col("C_Type") == "Vote")
    .filter(F.col("E3_relationship") == "CastFor")
    .orderBy(F.col("count").desc())
)
graphlet_count_df.show()

# +------+--------------+------+---------------+------+----------+---------------+------+----------+------+
# |A_Type|E_relationship|B_Type|E2_relationship|C_Type|C_VoteType|E3_relationship|D_Type|D_VoteType| count|
# +------+--------------+------+---------------+------+----------+---------------+------+----------+------+
# |  Post|       Answers|  Post|        CastFor|  Vote|    UpVote|        CastFor|  Vote|    UpVote|313807|
# |  Post|         Links|  Post|        CastFor|  Vote|    UpVote|        CastFor|  Vote|    UpVote|271944|
# |  Post|       Answers|  Post|        CastFor|  Vote|    UpVote|        CastFor|  Vote|   Unknown|  8159|
# |  Post|       Answers|  Post|        CastFor|  Vote|   Unknown|        CastFor|  Vote|    UpVote|  8159|
# |  Post|         Links|  Post|        CastFor|  Vote|    UpVote|        CastFor|  Vote|  DownVote|  6749|
# |  Post|         Links|  Post|        CastFor|  Vote|  DownVote|        CastFor|  Vote|    UpVote|  6749|
# |  Post|       Answers|  Post|        CastFor|  Vote|  DownVote|        CastFor|  Vote|    UpVote|  6586|
# |  Post|       Answers|  Post|        CastFor|  Vote|    UpVote|        CastFor|  Vote|  DownVote|  6586|
# |  Post|         Links|  Post|        CastFor|  Vote|  DownVote|        CastFor|  Vote|  DownVote|  4266|
# |  Post|       Answers|  Post|        CastFor|  Vote|  DownVote|        CastFor|  Vote|  DownVote|  4190|
# |  Post|         Links|  Post|        CastFor|  Vote|    UpVote|        CastFor|  Vote|   Unknown|  1617|
# |  Post|         Links|  Post|        CastFor|  Vote|   Unknown|        CastFor|  Vote|    UpVote|  1617|
# |  Post|       Answers|  Post|        CastFor|  Vote|   Unknown|        CastFor|  Vote|   Unknown|   919|
# |  Post|         Links|  Post|        CastFor|  Vote|   Unknown|        CastFor|  Vote|  DownVote|   317|
# |  Post|         Links|  Post|        CastFor|  Vote|  DownVote|        CastFor|  Vote|   Unknown|   317|
# |  Post|         Links|  Post|        CastFor|  Vote|   Unknown|        CastFor|  Vote|   Unknown|   239|
# |  Post|       Answers|  Post|        CastFor|  Vote|   Unknown|        CastFor|  Vote|  DownVote|   113|
# |  Post|       Answers|  Post|        CastFor|  Vote|  DownVote|        CastFor|  Vote|   Unknown|   113|
# |  Post|       Answers|  Post|        CastFor|  Vote|    UpVote|        CastFor|  Vote|      Spam|    92|
# |  Post|       Answers|  Post|        CastFor|  Vote|      Spam|        CastFor|  Vote|    UpVote|    92|
# +------+--------------+------+---------------+------+----------+---------------+------+----------+------+
# only showing top 20 rows

# G14: Cyclic Quadrilaterals
paths = g.find("(a)-[e]->(b); (b)-[e2]->(c); (c)-[e3]->(d); (d)-[e4]->(a)")
four_edge_count(paths).show()


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
