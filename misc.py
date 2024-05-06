from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

# Create a SparkSession
spark = SparkSession.builder \
    .appName("AveragePriceAnalysisForFictionGenre") \
    .getOrCreate()

# Load the CSV file into a DataFrame
df = spark.read.csv("/Users/zeeshanwaheed/Downloads/AmazonBooks-1.csv", header=True)

# Assuming the column names are "Genre", "Price", "Rank", and "Year"
genre_column = "Genre"
price_column = "Price"
rank_column = "Rank"
year_column = "Year"

# Define the range of years for analysis
start_year = 2009
end_year = 2020

# Loop through each year within the range
for year_to_analyze in range(start_year, end_year + 1):
    # Filter the DataFrame to select only rows with "Fiction" genre and the specified year
    fiction_year_df = df.filter((col(genre_column) == "Fiction") & (col(year_column) == str(year_to_analyze)))

    # Calculate the average price for top 10, 25, and 50 ranks
    for rank_limit in [10, 25, 50]:
        avg_price = fiction_year_df.filter(col(rank_column) <= rank_limit).select(avg(col(price_column))).collect()[0][0]
        print(f"Year: {year_to_analyze}, Top {rank_limit} Average Price for Fiction Genre: {avg_price}")

# Stop the SparkSession
spark.stop()
