import os
os.environ['JAVA_HOME'] = r'C:\Program Files\Zulu\zulu-17'
os.environ['HADOOP_HOME'] = r'C:\hadoop'
os.environ['PATH'] += r';C:\hadoop\bin'
os.environ['JAVA_TOOL_OPTIONS'] = (
    '--add-opens=java.base/javax.security.auth=ALL-UNNAMED '
    '--add-opens=java.base/java.lang=ALL-UNNAMED'
)
os.environ['PYSPARK_SUBMIT_ARGS'] = (
    '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 '
    'pyspark-shell'
)

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, explode, split, lower, length,
    count, avg, when, trim, regexp_replace
)
from pyspark.sql.types import StructType, StructField, StringType

# CONFIG
KAFKA_BROKER = 'localhost:9092'
TOPIC = 'social-media'

STOP_WORDS = [
    "the", "a", "an", "and", "or", "but", "in", "on", "at",
    "to", "for", "of", "is", "it", "that", "this", "with",
    "as", "by", "from", "are", "was", "were", "be", "been",
    "has", "have", "had", "not", "its", "he", "she", "they",
    "his", "her", "their", "our", "we", "you", "i", "will",
    "can", "may", "about", "over", "after", "also", "more"
]

# SPARK SESSION
spark = SparkSession.builder \
    .appName("SocialMediaStreaming") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# SCHEMA
schema = StructType([
    StructField("text",      StringType()),
    StructField("timestamp", StringType()),
    StructField("user",      StringType())
])

# READ FROM KAFKA
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# PARSE JSON 
parsed = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# foreachBatch — ALL transformations here
def print_all(batch_df, batch_id):

    # T2: Clean & Enrich
    enriched = batch_df \
    .withColumn("clean_text", lower(col("text"))) \
    .withColumn("clean_text", regexp_replace(col("clean_text"), r'<.*?>', ' ')) \
    .withColumn("clean_text", regexp_replace(col("clean_text"), r'[^a-z0-9\s]', ' ')) \
    .withColumn("clean_text", regexp_replace(col("clean_text"), r'\s+', ' ')) \
    .withColumn("clean_text", trim(col("clean_text"))) \
    .withColumn("text_length", length(col("clean_text"))) \
    .withColumn("size_label",
        when(col("text_length") > 200, "long")
        .when(col("text_length") > 100, "medium")
        .otherwise("short")
    )

    rows = enriched.collect()
    if not rows:
        print(f"  [Batch {batch_id}] No data yet...")
        return

    # T3: Word Count
    word_counts = enriched \
    .select(explode(split(col("clean_text"), r"\s+")).alias("word")) \
    .filter(col("word") != "") \
    .filter(length(col("word")) >= 2) \
    .filter(~col("word").rlike("^[0-9]+$")) \
    .filter(~col("word").isin(STOP_WORDS)) \
    .groupBy("word") \
    .count() \
    .orderBy("count", ascending=False) \
    .limit(10) \
    .collect()
    
    # T5: Size Distribution
    size_distribution = enriched \
        .groupBy("size_label") \
        .count() \
        .orderBy("count", ascending=False) \
        .collect()

    # Print Articles
    print("\n" + "=" * 70)
    print(f"  BATCH {batch_id} — {len(rows)} NEW ARTICLES")
    print("=" * 70)
    for row in rows:
        print(f"\n  Timestamp  : {row['timestamp']}")
        print(f"  Source     : {row['user']}")
        print(f"  Size       : {row['size_label']} ({row['text_length']} chars)")
        print(f"  Content    : {row['text']}")
        print("-" * 70)

    # Print Word Count
    print(f"\n  TOP KEYWORDS")
    print("=" * 70)
    for i, row in enumerate(word_counts, 1):
        bar = "█" * min(row['count'], 30)
        print(f"  {i:2}. {row['word']:<20} {bar} ({row['count']})")

 
    # Print Size Distribution
    print(f"\n  POST SIZE DISTRIBUTION")
    print("=" * 70)
    for row in size_distribution:
        bar = "█" * min(row['count'], 30)
        print(f"  {row['size_label']:<10} {bar} ({row['count']})")

    print("=" * 70 + "\n")

# START STREAMING
query = parsed \
    .writeStream \
    .foreachBatch(print_all) \
    .outputMode("append") \
    .start()

query.awaitTermination()


# OLD CODES

# .withColumn("text", trim(regexp_replace(col("text"), r'[^\w\s]', ''))) \
    # .withColumn("text_length", length(col("text"))) \
    # .withColumn("size_label",
    #     when(col("text_length") > 200, "long")
    #     .when(col("text_length") > 100, "medium")
    #     .otherwise("short")
    # )

# .select(explode(split(lower(col("text")), " ")).alias("word")) \
        # .filter(col("word") != "") \
        # .filter(~col("word").isin(STOP_WORDS)) \
        # .groupBy("word") \
        # .count() \
        # .orderBy("count", ascending=False) \
        # .limit(10) \
        # .collect()

# T4: User Activity
    # user_stats = enriched \
    #     .groupBy("user") \
    #     .agg(
    #         count("*").alias("post_count"),
    #         avg("text_length").alias("avg_length")
    #     ) \
    #     .orderBy("post_count", ascending=False) \
    #     .collect()

   # Print User Activity
    # print(f"\n  USER ACTIVITY")
    # print("=" * 70)
    # for row in user_stats:
    #     print(f"  {row['user']:<20} posts: {row['post_count']}  avg length: {row['avg_length']:.0f} chars")
