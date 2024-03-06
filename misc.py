import pyspark

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum

# Create a SparkSession
spark = SparkSession.builder \
    .appName("GenreReviewsAnalysis") \
    .getOrCreate()

# Load the CSV file into a DataFrame
df = spark.read.csv("/Users/zeeshanwaheed/Downloads/AmazonBooks-1.csv", header=True)

# Assuming the column names are "Genre", "Reviews", "Rank", and "Years"
genre_column = "Genre"
reviews_column = "Reviews"
rank_column = "Rank"
years_column = "Years"

# Define the genre to analyze
genre_to_analyze = "fiction"  # Change to "non-fiction" if needed

# Filter the DataFrame based on the genre
filtered_df = df.filter(col(genre_column) == genre_to_analyze)

# Calculate the average and total number of reviews for top 10, 25, and 50 ranks
top_10_avg_reviews = filtered_df.filter(col(rank_column) <= 10).select(avg(col(reviews_column))).collect()[0][0]
top_10_total_reviews = filtered_df.filter(col(rank_column) <= 10).select(sum(col(reviews_column))).collect()[0][0]

top_25_avg_reviews = filtered_df.filter(col(rank_column) <= 25).select(avg(col(reviews_column))).collect()[0][0]
top_25_total_reviews = filtered_df.filter(col(rank_column) <= 25).select(sum(col(reviews_column))).collect()[0][0]

top_50_avg_reviews = filtered_df.filter(col(rank_column) <= 50).select(avg(col(reviews_column))).collect()[0][0]
top_50_total_reviews = filtered_df.filter(col(rank_column) <= 50).select(sum(col(reviews_column))).collect()[0][0]

print(f"Genre: {genre_to_analyze}")
print("Top 10 Average Reviews:", top_10_avg_reviews)
print("Top 10 Total Reviews:", top_10_total_reviews)
print("Top 25 Average Reviews:", top_25_avg_reviews)
print("Top 25 Total Reviews:", top_25_total_reviews)
print("Top 50 Average Reviews:", top_50_avg_reviews)
print("Top 50 Total Reviews:", top_50_total_reviews)

# Stop the SparkSession
spark.stop()