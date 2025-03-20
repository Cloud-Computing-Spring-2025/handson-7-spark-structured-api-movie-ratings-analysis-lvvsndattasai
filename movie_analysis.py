from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, round, trim, lower, regexp_replace

# Initialize Spark session
spark = SparkSession.builder.appName("Movie Ratings Analysis").getOrCreate()

# Load dataset
df = spark.read.csv("movie_ratings_data.csv", header=True, inferSchema=True)

### Task 1: Detect Binge-Watching Patterns ###
binge_watchers = df.filter(col("IsBingeWatched") == True) \
    .groupBy("AgeGroup") \
    .agg(count("*").alias("Binge Watchers"))

total_watchers = df.groupBy("AgeGroup").agg(count("*").alias("Total Watchers"))
binge_percentage = binge_watchers.join(total_watchers, "AgeGroup") \
    .withColumn("Percentage", round((col("Binge Watchers") / col("Total Watchers")) * 100, 2))

binge_percentage.write.mode("overwrite").option("header", "true").csv("binge_watchers_output")

### Task 2: Identify Churn Risk Users ###
# Clean SubscriptionStatus column (remove spaces, emojis, hidden characters)
df = df.withColumn("SubscriptionStatus", lower(trim(regexp_replace(col("SubscriptionStatus"), "[^a-zA-Z]", ""))))

# Filter for users who have 'canceled' status and watch time < 100
churn_risk_users = df.filter((col("SubscriptionStatus") == "canceled") & (col("WatchTime") < 100)) \
    .agg(count("*").alias("Total Users"))

churn_risk_users.write.mode("overwrite").option("header", "true").csv("churn_risk_output")

### Task 3: Trend Analysis Over the Years ###
trend_analysis = df.filter(col("WatchedYear").isNotNull()) \
    .groupBy("WatchedYear") \
    .agg(count("*").alias("Movies Watched")) \
    .orderBy("WatchedYear")

trend_analysis.write.mode("overwrite").option("header", "true").csv("trend_analysis_output")

print("Analysis completed. Output files generated.")

