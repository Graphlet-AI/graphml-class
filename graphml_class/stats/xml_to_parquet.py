# Convert some Stack Exchange XML files to Parquet format

#
# Usage: pyspark --packages com.databricks:spark-xml_2.12:0.18.0
#

import os
import re
from typing import List

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame, SparkSession


# This is actually already set in Docker, just reminding you Java is needed
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"

# This doesn't work from here, you have to do this from the CLI via pyspark or spark-submit
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages com.databricks:spark-xml_2.12:0.18.0"
    "--driver-memory 4g pyspark-shell "
    "--executor-memory 4g pyspark-shell "
    "--driver-java-options='-Xmx4g -Xms4g' "
)

#
# Section Le Spark...
#

# Initialize a SparkSession. You can configre SparkSession via: .config("spark.some.config.option", "some-value")
spark: SparkSession = SparkSession.builder.appName(
    "Big Graph Builder"
).getOrCreate()  # Set app name  # Get or create the SparkSession


def remove_prefix(df: DataFrame) -> DataFrame:
    """Remove the _ prefix that Spark-XML adds to all attributes"""
    field_names = [x.name for x in df.schema]
    new_field_names = [x[1:] for x in field_names]
    s = []

    # Substitute the old name for the new one
    for old, new in zip(field_names, new_field_names):
        s.append(F.col(old).alias(new))
    return df.select(s)


@F.udf(returnType=T.ArrayType(T.StringType()))
def split_tags(tags: str) -> List[str]:
    if not tags:
        return []
    # Remove < and > and split into array
    return re.findall(r"<([^>]+)>", tags)


# Use Spark-XML to split the XML file into records
posts_df: DataFrame = (
    spark.read.format("xml")
    .options(rowTag="row")
    .options(rootTag="posts")
    .load("data/stats.meta.stackexchange.com/Posts.xml")
)

# Remove the _ prefix from field names
posts_df: DataFrame = remove_prefix(posts_df)

# Create a list of tags
posts_df: DataFrame = (
    posts_df.withColumn(
        "ParsedTags", F.split(F.regexp_replace(F.col("Tags"), "^\\||\\|$", ""), "\\|")
    )
    .drop("Tags")
    .withColumnRenamed("ParsedTags", "Tags")
)

# Write the DataFrame out to Parquet format
posts_df.repartition(1).write.mode("overwrite").parquet(
    "data/stats.meta.stackexchange.com/Posts.parquet"
)


# Use Spark-XML to split the XML file into records
users_df = (
    spark.read.format("xml")
    .options(rowTag="row")
    .options(rootTag="users")
    .load("data/stats.meta.stackexchange.com/Users.xml")
)

# Remove the _ prefix from field names
users_df = remove_prefix(users_df)

# Write the DataFrame out to Parquet format
users_df.repartition(1).write.mode("overwrite").parquet(
    "data/stats.meta.stackexchange.com/Users.parquet"
)


# Use Spark-XML to split the XML file into records
votes_df = (
    spark.read.format("xml")
    .options(rowTag="row")
    .options(rootTag="votes")
    .load("data/stats.meta.stackexchange.com/Votes.xml")
)

# Remove the _ prefix from field names
votes_df = remove_prefix(votes_df)

# Add a VoteType column
votes_df = votes_df.withColumn(
    "VoteType",
    F.when(F.col("VoteTypeId") == 2, "UpVote")
    .when(F.col("VoteTypeId") == 3, "DownVote")
    .when(F.col("VoteTypeId") == 4, "Favorite")
    .when(F.col("VoteTypeId") == 5, "Close")
    .when(F.col("VoteTypeId") == 6, "Reopen")
    .when(F.col("VoteTypeId") == 7, "BountyStart")
    .when(F.col("VoteTypeId") == 8, "BountyClose")
    .when(F.col("VoteTypeId") == 9, "Deletion")
    .when(F.col("VoteTypeId") == 10, "Undeletion")
    .when(F.col("VoteTypeId") == 11, "Spam")
    .when(F.col("VoteTypeId") == 12, "InformModerator")
    .otherwise("Unknown"),
)

# Write the DataFrame out to Parquet format
votes_df.repartition(1).write.mode("overwrite").parquet(
    "data/stats.meta.stackexchange.com/Votes.parquet"
)

# Use Spark-XML to split the XML file into records
comments_df = (
    spark.read.format("xml")
    .options(rowTag="row")
    .options(rootTag="comments")
    .load("data/stats.meta.stackexchange.com/Comments.xml")
)

# Remove the _ prefix from field names
comments_df = remove_prefix(comments_df)

# Write the DataFrame out to Parquet format
comments_df.repartition(1).write.mode("overwrite").parquet(
    "data/stats.meta.stackexchange.com/Comments.parquet"
)

# Use Spark-XML to split the XML file into records
badges_df = (
    spark.read.format("xml")
    .options(rowTag="row")
    .options(rootTag="badges")
    .load("data/stats.meta.stackexchange.com/Badges.xml")
)

# Remove the _ prefix from field names
badges_df = remove_prefix(badges_df)

# Write the DataFrame out to Parquet format
badges_df.repartition(1).write.mode("overwrite").parquet(
    "data/stats.meta.stackexchange.com/Badges.parquet"
)

# Use Spark-XML to split the XML file into records
posthistory_df = (
    spark.read.format("xml")
    .options(rowTag="row")
    .options(rootTag="posthistory")
    .load("data/stats.meta.stackexchange.com/PostHistory.xml")
)

# Remove the _ prefix from field names
posthistory_df = remove_prefix(posthistory_df)

# Write the DataFrame out to Parquet format
posthistory_df.repartition(1).write.mode("overwrite").parquet(
    "data/stats.meta.stackexchange.com/PostHistory.parquet"
)

# Use Spark-XML to split the XML file into records
postlinks_df = (
    spark.read.format("xml")
    .options(rowTag="row")
    .options(rootTag="postlinks")
    .load("data/stats.meta.stackexchange.com/PostLinks.xml")
)

# Remove the _ prefix from field names
postlinks_df = remove_prefix(postlinks_df)

# Write the DataFrame out to Parquet format
postlinks_df.repartition(1).write.mode("overwrite").parquet(
    "data/stats.meta.stackexchange.com/PostLinks.parquet"
)

# Use Spark-XML to split the XML file into records
tags_df = (
    spark.read.format("xml")
    .options(rowTag="row")
    .options(rootTag="tags")
    .load("data/stats.meta.stackexchange.com/Tags.xml")
)

# Remove the _ prefix from field names
tags_df = remove_prefix(tags_df)

# Write the DataFrame out to Parquet format
tags_df.repartition(1).write.mode("overwrite").parquet(
    "data/stats.meta.stackexchange.com/Tags.parquet"
)
