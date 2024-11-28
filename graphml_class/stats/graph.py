#!/usr/bin/env pyspark --packages graphframes:graphframes:0.8.3-spark3.5-s_2.12 --driver-memory 4g --executor-memory 4g

import os
from typing import List

import pandas as pd
import pyspark.sql.functions as F
from pyspark import SparkContext
from pyspark.sql import DataFrame, SparkSession

# This is actually already set in Docker, just reminding you Java is needed
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"

# Setup PySpark to use the GraphFrames jar package from maven central
os.environ["PYSPARK_SUBMIT_ARGS"] = (
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


print("Loading data for stats.meta.stackexchange.com ...")

#
# Load the Posts...
#
posts_df: DataFrame = spark.read.parquet(
    "data/stats.meta.stackexchange.com/Posts.parquet"
).withColumn("Type", F.lit("Post"))

print(f"\nTotal Posts:     {posts_df.count():,}")

#
# Load the PostLinks...
#
post_links_df: DataFrame = spark.read.parquet(
    "data/stats.meta.stackexchange.com/PostLinks.parquet"
).withColumn("Type", F.lit("PostLinks"))
print(f"Total PostLinks: {post_links_df.count():,}")

#
# Load the Comments...
#
comments_df: DataFrame = spark.read.parquet(
    "data/stats.meta.stackexchange.com/Comments.parquet"
).withColumn("Type", F.lit("Comment"))
print(f"Total Comments:  {comments_df.count():,}")

#
# Load the Users...
#
users_df: DataFrame = spark.read.parquet(
    "data/stats.meta.stackexchange.com/Users.parquet"
).withColumn("Type", F.lit("User"))
print(f"Total Users:     {users_df.count():,}")

#
# Load the Votes...
#
votes_df: DataFrame = spark.read.parquet(
    "data/stats.meta.stackexchange.com/Votes.parquet"
).withColumn("Type", F.lit("Vote"))
print(f"Total Votes:     {votes_df.count():,}")

#
# Load the Tags...
#
tags_df: DataFrame = spark.read.parquet(
    "data/stats.meta.stackexchange.com/Tags.parquet"
).withColumn("Type", F.lit("Tag"))
print(f"Total Tags:      {tags_df.count():,}")

#
# Load the Badges...
#
badges_df: DataFrame = spark.read.parquet(
    "data/stats.meta.stackexchange.com/Badges.parquet"
).withColumn("Type", F.lit("Badge"))
print(f"Total Badges:    {badges_df.count():,}\n")


#
# Form the nodes from the UNION of posts, users, votes and their combined schemas
#
all_cols: List[str] = list(
    set(
        list(zip(posts_df.columns, posts_df.schema))
        + list(zip(post_links_df.columns, post_links_df.schema))
        + list(zip(comments_df.columns, comments_df.schema))
        + list(zip(users_df.columns, users_df.schema))
        + list(zip(votes_df.columns, votes_df.schema))
        + list(zip(tags_df.columns, tags_df.schema))
        + list(zip(badges_df.columns, badges_df.schema))
    )
)
all_column_names: List[str] = sorted([x[0] for x in all_cols])


def add_missing_columns(df, all_cols):
    """Add any missing columns from any DataFrame among several we want to merge."""
    for col_name, schema_field in all_cols:
        if col_name not in df.columns:
            df: DataFrame = df.withColumn(col_name, F.lit(None).cast(schema_field.dataType))
    return df


# Now apply this function to each of your DataFrames to get a consistent schema
posts_df: DataFrame = add_missing_columns(posts_df, all_cols).select(all_column_names)
post_links_df: DataFrame = add_missing_columns(post_links_df, all_cols).select(all_column_names)
users_df: DataFrame = add_missing_columns(users_df, all_cols).select(all_column_names)
votes_df: DataFrame = add_missing_columns(votes_df, all_cols).select(all_column_names)
tags_df: DataFrame = add_missing_columns(tags_df, all_cols).select(all_column_names)
badges_df: DataFrame = add_missing_columns(badges_df, all_cols).select(all_column_names)
assert (
    set(posts_df.columns)
    == set(post_links_df.columns)
    == set(users_df.columns)
    == set(votes_df.columns)
    == set(all_column_names)
    == set(tags_df.columns)
    == set(badges_df.columns)
)

# Now union them together and remove duplicates
nodes_df: DataFrame = (
    posts_df.unionByName(post_links_df)
    .unionByName(users_df)
    .unionByName(votes_df)
    .unionByName(tags_df)
    .unionByName(badges_df)
    .distinct()
)
print(f"Total distinct nodes: {nodes_df.count():,}")

# Now add a unique ID field
nodes_df: DataFrame = nodes_df.withColumn("id", F.expr("uuid()")).select("id", *all_column_names)

#
# Store the nodes to disk, reload and cache
#
NODES_PATH: str = "data/stats.meta.stackexchange.com/Nodes.parquet"

# Write to disk and load back again
nodes_df.write.mode("overwrite").parquet(NODES_PATH)
nodes_df: DataFrame = spark.read.parquet(NODES_PATH)

nodes_df.select("id", "Type").groupBy("Type").count().show()

# +---------+------+
# |     Type|count |
# +---------+------+
# |PostLinks| 1,344|
# |     Vote|41,851|
# |     User|36,653|
# |    Badge|41,573|
# |      Tag|   145|
# |     Post| 4,930|
# +---------+------+

# Helps performance of GraphFrames' algorithms
nodes_df: DataFrame = nodes_df.cache()

# Make sure we have the right columns and cached data
posts_df: DataFrame = nodes_df.filter(nodes_df.Type == "Post").cache()
post_links_df: DataFrame = nodes_df.filter(nodes_df.Type == "PostLinks").cache()
users_df: DataFrame = nodes_df.filter(nodes_df.Type == "User").cache()
votes_df: DataFrame = nodes_df.filter(nodes_df.Type == "Vote").cache()
tags_df: DataFrame = nodes_df.filter(nodes_df.Type == "Tag").cache()
badges_df: DataFrame = nodes_df.filter(nodes_df.Type == "Badge").cache()


#
# Building blocks: separate the questions and answers
#

# Do the questions look ok? Questions have NO parent ID and DO have a Title
questions_df: DataFrame = posts_df[posts_df.ParentId.isNull()].cache()
print(f"\nTotal questions: {questions_df.count():,}\n")

questions_df.select("ParentId", "Title", "Body").show(10)

# Answers DO have a ParentId parent post and no Title
answers_df: DataFrame = posts_df[posts_df.ParentId.isNotNull()].cache()
print(f"\nTotal answers: {answers_df.count():,}\n")

answers_df.select("ParentId", "Title", "Body").show(10)


#
# Build the edges DataFrame:
#
# * [Vote]--CastFor-->[Post]
# * [User]--Asks-->[Post]
# * [User]--Answers-->[Post]
# * [Post]--Answers-->[Post]
# * [Tag]--Tags-->[Post]
# * [User]--Earns-->[Badge]
# * [Post]--Links-->[Post]
#

#
# Remember: 'src', 'dst' and 'relationship' are standard edge fields in GraphFrames
# Remember: we must produce src/dst based on lowercase 'id' UUID, not 'Id' which is Stack Overflow's integer.
#


#
# Create a [Vote]--CastFor-->[Post] edge
#
src_vote_df: DataFrame = votes_df.select(
    F.col("id").alias("src"),
    F.col("Id").alias("VoteId"),
    # Everything has all the fields - should build from base records but need UUIDs
    F.col("PostId").alias("VotePostId"),
)
cast_for_edge_df: DataFrame = src_vote_df.join(
    posts_df, src_vote_df.VotePostId == posts_df.Id
).select(
    # 'src' comes from the votes' 'id'
    "src",
    # 'dst' comes from the posts' 'id'
    F.col("id").alias("dst"),
    # All edges have a 'relationship' field
    F.lit("CastFor").alias("relationship"),
)
print(f"Total CastFor edges: {cast_for_edge_df.count():,}")
print(f"Percentage of linked votes: {cast_for_edge_df.count() / votes_df.count():.2%}\n")


#
# Create a [User]--Asks-->[Post] edge
#
user_asks_edges_df: DataFrame = questions_df.select(
    F.col("OwnerUserId").alias("src"),
    F.col("id").alias("dst"),
    F.lit("Asks").alias("relationship"),
)
print(f"Total Asks edges: {user_asks_edges_df.count():,}")
print(
    f"Percentage of asked questions linked to users: {user_asks_edges_df.count() / questions_df.count():.2%}\n"
)

user_answers_edges_df: DataFrame = answers_df.select(
    F.col("OwnerUserId").alias("src"),
    F.col("id").alias("dst"),
    F.lit("Answers").alias("relationship"),
)
print(f"Total User Answers edges: {user_answers_edges_df.count():,}")
print(
    f"Percentage of asked questions linked to users: {user_answers_edges_df.count() / answers_df.count():.2%}\n"
)


#
# Create a [Post, answer]--Answers-->[Post, question] edge
#
src_answers_df: DataFrame = answers_df.select(
    F.col("id").alias("src"),
    F.col("Id").alias("AnswerId"),
    F.col("ParentId").alias("AnswerParentId"),
)
post_answers_edges_df: DataFrame = src_answers_df.join(
    posts_df, src_answers_df.AnswerParentId == posts_df.Id
).select(
    # 'src' comes from the answers' 'id'
    "src",
    # 'dst' comes from the posts' 'id'
    F.col("id").alias("dst"),
    # All edges have a 'relationship' field
    F.lit("Answers").alias("relationship"),
)
print(f"Total Posts Answers edges: {post_answers_edges_df.count():,}")
print(f"Percentage of linked answers: {post_answers_edges_df.count() / answers_df.count():.2%}\n")


#
# Create a [Tag]--Tags-->[Post] edge
#
src_tags_df: DataFrame = posts_df.select(
    F.col("id").alias("dst"),
    # First remove leading/trailing < and >, then split on "><"
    F.explode("Tags").alias("Tag"),
)
tags_edge_df: DataFrame = src_tags_df.join(tags_df, src_tags_df.Tag == tags_df.TagName).select(
    # 'src' comes from the posts' 'id'
    "dst",
    # 'dst' comes from the tags' 'id'
    F.col("id").alias("src"),
    # All edges have a 'relationship' field
    F.lit("Tags").alias("relationship"),
)
print(f"Total Tags edges: {tags_edge_df.count():,}")
print(f"Percentage of linked tags: {tags_edge_df.count() / posts_df.count():.2%}\n")


#
# Create a [User]--Earns-->[Badge] edge
#
earns_edges_df: DataFrame = badges_df.select(
    F.col("UserId").alias("src"),
    F.col("id").alias("dst"),
    F.lit("Earns").alias("relationship"),
)
print(f"Total Earns edges: {earns_edges_df.count():,}")
print(f"Percentage of earned badges: {earns_edges_df.count() / badges_df.count():.2%}\n")

#
# Create a [Post]--Links-->[Post] edge
#
trim_links_df: DataFrame = post_links_df.select(
    F.col("PostId").alias("SrcPostId"), F.col("RelatedPostId").alias("DstPostId")
)
links_src_edge_df: DataFrame = trim_links_df.join(
    posts_df, trim_links_df.SrcPostId == posts_df.Id
).select(
    # 'dst' comes from the posts' 'id'
    F.col("id").alias("src"),
    "DstPostId",
)
links_edge_df = links_src_edge_df.join(posts_df, links_src_edge_df.DstPostId == posts_df.Id).select(
    "src",
    # 'src' comes from the posts' 'id'
    F.col("id").alias("dst"),
    # All edges have a 'relationship' field
    F.lit("Links").alias("relationship"),
)
print(f"Total Links edges: {links_edge_df.count():,}")
print(f"Percentage of linked posts: {links_edge_df.count() / post_links_df.count():.2%}\n")


#
# Combine all the edges together into one relationships DataFrame
#
relationships_df: DataFrame = (
    cast_for_edge_df.unionByName(user_asks_edges_df)
    .unionByName(user_answers_edges_df)
    .unionByName(post_answers_edges_df)
    .unionByName(tags_edge_df)
    .unionByName(earns_edges_df)
    .unionByName(links_edge_df)
)

relationships_df.groupBy("relationship").count().show()

# +------------+------+
# |relationship|count |
# +------------+------+
# |     CastFor|40,701|
# |        Asks| 2,025|
# |     Answers| 2,978|
# |        Tags| 4,427|
# |       Earns|43,029|
# |       Links| 1,268|
# +------------+------+

EDGES_PATH: str = "data/stats.meta.stackexchange.com/Edges.parquet"

# Write to disk and back again
relationships_df.write.mode("overwrite").parquet(EDGES_PATH)
