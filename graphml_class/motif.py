import os
import resource
from uuid import uuid4

import pandas as pd
import pyspark.sql.functions as F
from graphframes import GraphFrame
from pyspark.sql import SparkSession

# This is actually already set in Docker, just reminding you Java is needed
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"

# Setup PySpark to use the GraphFrames jar package from maven central
os.environ[
    "PYSPARK_SUBMIT_ARGS"
] = "--packages graphframes:graphframes:0.8.3-spark3.5-s_2.12 pyspark-shell"

# Show all the rows in a pd.DataFrame
pd.set_option("display.max_columns", None)


def get_memory_usage():
    """Return the resident memory usage in GB."""
    return (
        resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        + resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
    ) / (1024 * 1024)


def print_memory_usage():
    """Print the resident memory usage in GB."""
    print(f"Memory usage: {get_memory_usage():.2f} GB")


#
# Initialize a SparkSession. You can configre SparkSession via: .config("spark.some.config.option", "some-value")
#
spark = (
    SparkSession.builder.appName("Stack Overflow Motif Analysis")
    # Lets the Id:(Stack Overflow int) and id:(GraphFrames ULID) coexist
    .config("spark.sql.caseSensitive", True)
    # Single node mode - 128GB machine
    .config("spark.driver.memory", "96g").getOrCreate()
)


#
# Load the Posts...
#
posts_df = spark.read.parquet("data/stats.meta.stackexchange.com/Posts.parquet").withColumn(
    "Type", F.lit("Post")
)

print(f"\nTotal stats.meta.stackexchange.com Posts: {posts_df.count():,}\n")


#
# Load the Users...
#
users_df = spark.read.parquet("data/stats.meta.stackexchange.com/Users.parquet").withColumn(
    "Type", F.lit("User")
)
print(f"\nTotal Users: {users_df.count():,}\n")


#
# Load the Votes...
#
votes_df = spark.read.parquet("data/stats.meta.stackexchange.com/Votes.parquet").withColumn(
    "Type", F.lit("Vote")
)
print(f"Total Votes: {votes_df.count():,}")


#
# Form the nodes from the UNION of posts, users, votes and their combined schemas
#
all_cols = set(
    list(zip(posts_df.columns, posts_df.schema))
    + list(zip(users_df.columns, users_df.schema))
    + list(zip(votes_df.columns, votes_df.schema))
)


def add_missing_columns(df, all_cols):
    """Add any missing columns from any DataFrame among several we want to merge."""
    for col_name, schema_field in all_cols:
        if col_name not in df.columns:
            df = df.withColumn(col_name, F.lit(None).cast(schema_field.dataType))
    return df


# Now apply this function to each of your DataFrames to get a consistent schema
posts_df = add_missing_columns(posts_df, all_cols)
users_df = add_missing_columns(users_df, all_cols)
votes_df = add_missing_columns(votes_df, all_cols)
assert set(posts_df.columns) == set(users_df.columns) == set(votes_df.columns)

# Now union them together and remove duplicates
nodes_df = posts_df.unionByName(users_df).unionByName(votes_df).distinct()
print(f"Total distinct nodes: {nodes_df.count():,}")

# Now add a unique ID field
nodes_df = nodes_df.withColumn("id", F.expr("uuid()"))


#
# Store the nodes to disk, reload and cache
#
NODES_PATH = "data/stats.meta.stackexchange.com/Nodes.parquet"

# Write to disk and back again
nodes_df.write.mode("overwrite").parquet(NODES_PATH)
nodes_df = spark.read.parquet(NODES_PATH)

# Helps performance of GraphFrames' algorithms
nodes_df = nodes_df.cache()


#
# Build the edges DataFrame: VotedFor, AskedBy, AnsweredBy.
# Remember: 'src', 'dst' and 'relationship' are standard edge fields in GraphFrames
# Remember: we must produce src/dst based on lowercase 'id' UUID, not 'Id' which is Stack Overflow's integer.
#

# Make sure we have the right columns and cached data
votes_df = nodes_df.filter(nodes_df.Type == "Vote")
posts_df = nodes_df.filter(nodes_df.Type == "Post")
users_df = nodes_df.filter(nodes_df.Type == "User")


#
# Building blocks: separate the questions and answers
#

# Do the questions look ok? Questions have NO parent ID and DO have a Title
questions_df = posts_df[posts_df.ParentId.isNull()]
print(f"\nTotal questions: {questions_df.count():,}\n")

questions_df.select("ParentId", "Title", "Body").show(10)

# Answers DO have a ParentId parent post and no Title
answers_df = posts_df[posts_df.ParentId.isNotNull()]
print(f"\nTotal answers: {answers_df.count():,}\n")

answers_df.select("ParentId", "Title", "Body").show(10)


#
# Create a [Vote]--VotedFor-->[Post] edge
#
src_vote_df = votes_df.select(
    F.col("id").alias("src"),
    F.col("Id").alias("VoteId"),
    # Everything has all the fields - should build from base records but need UUIDs
    F.col("PostId").alias("VotePostId"),
)
voted_for_edge_df = (
    src_vote_df.join(posts_df, src_vote_df.VotePostId == posts_df.Id)
    .select(
        # 'src' comes from the votes' 'id'
        "src",
        # 'dst' comes from the posts' 'id'
        F.col("id").alias("dst"),
        # All edges have a 'relationship' field
        F.lit("VotedFor").alias("relationship"),
    )
    .cache()
)
print(f"Total edges: {voted_for_edge_df.count():,}")


#
# Create a answer[Post]--Answers-->question[Post] edge
#
src_answers_df = answers_df.select(
    F.col("id").alias("src"),
    F.col("Id").alias("AnswerId"),
    F.col("ParentId").alias("AnswerParentId"),
)
answered_edges_df = (
    src_answers_df.join(posts_df, src_answers_df.AnswerParentId == posts_df.Id)
    .select(
        # 'src' comes from the answers' 'id'
        "src",
        # 'dst' comes from the posts' 'id'
        F.col("id").alias("dst"),
        # All edges have a 'relationship' field
        F.lit("Answers").alias("relationship"),
    )
    .cache()
)
print(f"\nTotal Answered edges: {answered_edges_df.count():,}\n")


#
# Create a [User]--Posted-->[Post] edge
#
src_posts_df = posts_df.select(
    F.col("id").alias("src"),
    F.col("Id").alias("PostPostId"),
    F.col("OwnerUserId").alias("PostOwnerUserId"),
)

posted_edge_df = (
    src_posts_df.join(users_df, src_posts_df.PostOwnerUserId == users_df.Id)
    .select(
        # 'src' comes from the posts' 'id'
        "src",
        # 'dst' comes from the users' 'id'
        F.col("id").alias("dst"),
        # All edges have a 'relationship' field
        F.lit("Posted").alias("relationship"),
    )
    .cache()
)
print(f"Total edges: {posted_edge_df.count():,}")
