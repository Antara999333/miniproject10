from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    # Initialize a Spark session
    spark = SparkSession.builder.appName("SparkSQLExample").getOrCreate()

    # Load your data (replace 'your_data_file.csv' with the actual file)
    df = spark.read.csv("airline-safety.csv", header=True, inferSchema=True)

    # Create a temporary view
    df.createOrReplaceTempView("airlinesafety")

    # Perform a data transformation using Spark SQL with a filter condition
    transformed_df = spark.sql("""
        SELECT *
        FROM airlinesafety
        WHERE incidents_85_99 < 20
    """)

    # Show the transformed data
    transformed_df.show()

    # You can also save the transformed data to a new file if needed.
    # transformed_df.write.csv("transformed_data.csv", header=True, mode="overwrite")

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()

