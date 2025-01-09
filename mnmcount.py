import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import count

# Main entry point of the script
if __name__ == "__main__":
    # Check if the required argument (file path) is provided
    if len(sys.argv) != 2:
        print("Usage: mmcount <file>", file=sys.stderr)
        sys.exit(-1)

    # Initialize SparkSession
    spark = (SparkSession
             .builder
             .appName("PythonMnMCount")
             .getOrCreate())

    # Set the log level to WARN to reduce verbosity
    spark.sparkContext.setLogLevel("WARN")

    # File path for the input M&M data
    mnm_file = sys.argv[1]

    # Load the CSV file into a DataFrame
    mnm_df = (spark.read.format("csv")
              .option("header", "true")  # First row contains column headers
              .option("inferSchema", "true")  # Automatically infer data types
              .load(mnm_file))

    # Aggregation: Group by State and Color, and count occurrences of each combination
    count_mnm_df = (mnm_df
                    .select("State", "Color", "Count")  # Select relevant columns
                    .groupBy("State", "Color")  # Group by State and Color
                    .agg(count("Count").alias("Total"))  # Count occurrences
                    .orderBy("Total", ascending=False))  # Sort by Total in descending order

    # Show the results for all states and colors
    count_mnm_df.show(n=60, truncate=False)

    # Print the total number of rows in the aggregated DataFrame
    print("Total Rows = %d" % (count_mnm_df.count()))

    # Filter for California (CA) data and perform similar aggregation
    ca_count_mnm_df = (mnm_df
                       .select("State", "Color", "Count")  # Select relevant columns
                       .where(mnm_df.State == "CA")  # Filter rows where State is "CA"
                       .groupBy("State", "Color")  # Group by State and Color
                       .agg(count("Count").alias("Total"))  # Count occurrences
                       .orderBy("Total", ascending=False))  # Sort by Total in descending order

    # Show the results for California
    ca_count_mnm_df.show(n=10, truncate=False)

    # Stop the SparkSession to release resources
    spark.stop()
