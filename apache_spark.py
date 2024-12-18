# Importing required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, year

# Create a Spark session
spark = SparkSession.builder.appName("NetflixEDA").getOrCreate()

# Load the dataset
data_path = "netflix_titles.csv"  # Update the path if needed
df = spark.read.csv(data_path, header=True, inferSchema=True)

# Show dataset schema
df.printSchema()

# Show the first few rows
df.show(10)

# Count the total number of records
print(f"Total number of records: {df.count()}")

# Data Cleaning
# Remove duplicates
df_cleaned = df.dropDuplicates()

# Drop rows with missing values
df_cleaned = df_cleaned.na.drop()

print(f"Number of records after cleaning: {df_cleaned.count()}")

# Exploratory Data Analysis (EDA)

# 1. Distribution of content types (Movies vs TV Shows)
content_count = df_cleaned.groupBy("type").count().orderBy(desc("count"))
print("Content type distribution:")
content_count.show()

# 2. Top 5 genres (based on listed_in column)
genre_count = df_cleaned.selectExpr("explode(split(listed_in, ', ')) as genre") \
    .groupBy("genre").count().orderBy(desc("count")).limit(5)
print("Top 5 genres:")
genre_count.show()

# 3. Number of titles released each year
release_year_count = df_cleaned.withColumn("release_year", year(col("date_added"))) \
    .groupBy("release_year").count().orderBy(desc("release_year"))
print("Number of titles released each year:")
release_year_count.show()

# 4. Country producing the most content
country_count = df_cleaned.groupBy("country").count().orderBy(desc("count")).limit(1)
print("Country producing the most content:")
country_count.show()

# Stop the Spark session
spark.stop()