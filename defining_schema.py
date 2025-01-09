# Import necessary modules from PySpark
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, concat, desc

# Define the schema for the DataFrame
schema = """
    `ID` INT, 
    `First` STRING, 
    `Last` STRING, 
    `Url` STRING, 
    `Published` STRING, 
    `Hits` INT, 
    `Campaigns` ARRAY<STRING>
"""

# Define the data to be used in the DataFrame
data = [
    [1, "Jules", "Damji", "https://tinyurl.1", "1/4/2016", 4535, ["twitter", "LinkedIn"]],
    [2, "Brooke", "Wenig", "https://tinyurl.2", "5/5/2018", 8908, ["twitter", "LinkedIn"]],
    [3, "Denny", "Lee", "https://tinyurl.3", "6/7/2019", 7659, ["web", "twitter", "FB", "LinkedIn"]],
    [4, "Tathagata", "Das", "https://tinyurl.4", "5/12/2018", 10568, ["twitter", "FB"]],
    [5, "Matei", "Zaharia", "https://tinyurl.5", "5/14/2014", 40578, ["web", "twitter", "FB", "LinkedIn"]],
    [6, "Reynold", "Xin", "https://tinyurl.6", "3/2/2015", 25568, ["twitter", "LinkedIn"]]
]

# Main block to initialize SparkSession and execute transformations
if __name__ == "__main__":
    # Initialize the SparkSession
    spark = (SparkSession
             .builder
             .appName("Defining Schema Example")
             .getOrCreate())

    # Create a DataFrame with the defined schema and data
    blogs_df = spark.createDataFrame(data, schema)

    # Add a new column 'Big Hitters' to indicate if Hits are greater than 10,000
    blogs_df = blogs_df.withColumn("Big Hitters", expr("Hits > 10000"))

    # Add a new column 'AuthorsID' by concatenating the first name twice and the ID
    blogs_df = blogs_df.withColumn("AuthorsID", concat(expr("First"), expr("First"), expr("ID")))

    # Sort the DataFrame by 'ID' column in descending order
    blogs_df = blogs_df.sort(desc("ID"))

    # Show the resulting DataFrame
    blogs_df.show()

    # Print the schema of the resulting DataFrame for verification
    print("Schema of the DataFrame:")
    blogs_df.printSchema()
