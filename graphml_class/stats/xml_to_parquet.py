# Convert some Stack Exchange XML files to Parquet format
#
# Usage: pyspark --packages com.databricks:spark-xml_2.13:0.17.0
#

import pyspark.sql.functions as F
from pyspark.sql import SparkSession


def remove_prefix(df):
    """Remove the _ prefix that Spark-XML adds to all attributes"""
    field_names = [x.name for x in df.schema]
    new_field_names = [x[1:] for x in field_names]
    s = []

    # Substitute the old name for the new one
    for old, new in zip(field_names, new_field_names):
        s.append(F.col(old).alias(new))
    return df.select(s)


# Initialize a SparkSession. You can configre SparkSession via: .config("spark.some.config.option", "some-value")
spark = SparkSession.builder.appName(
    "Big Graph Builder"
).getOrCreate()  # Set app name  # Get or create the SparkSession


# Use Spark-XML to split the XML file into records
posts_df = (
    spark.read.format("xml")
    .options(rowTag="row")
    .options(rootTag="posts")
    .load("data/stats.meta.stackexchange.com/Posts.xml")
)

# Remove the _ prefix from field names
posts_df = remove_prefix(posts_df)

# Write the DataFrame out to Parquet format
posts_df.write.mode("overwrite").parquet("data/stats.meta.stackexchange.com/Posts.parquet")


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
users_df.write.mode("overwrite").parquet("data/stats.meta.stackexchange.com/Users.parquet")


# Use Spark-XML to split the XML file into records
votes_df = (
    spark.read.format("xml")
    .options(rowTag="row")
    .options(rootTag="votes")
    .load("data/stats.meta.stackexchange.com/Votes.xml")
)

# Remove the _ prefix from field names
votes_df = remove_prefix(votes_df)

# Write the DataFrame out to Parquet format
votes_df.write.mode("overwrite").parquet("data/stats.meta.stackexchange.com/Votes.parquet")
