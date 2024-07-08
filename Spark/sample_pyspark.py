# Import necessary libraries
from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("PySpark Example") \
    .getOrCreate()

# Define the file paths
csv_file_path = "F:/NucleusTeq/Assignment/Spark/sample_data.csv"
json_file_path = "F:/NucleusTeq/Assignment/Spark/sample_data.json"
text_file_path = "F:/NucleusTeq/Assignment/Spark/sample_text.txt"

# Read a text file and create an RDD
text_rdd = spark.sparkContext.textFile(text_file_path)
print("Text File RDD:")
print(text_rdd.collect())

# Read a CSV file and create a DataFrame
csv_df = spark.read.csv(csv_file_path, header=True, inferSchema=True)
print("CSV DataFrame:")
csv_df.show()

# Read a JSON file and create a DataFrame
json_df = spark.read.json(json_file_path)
print("JSON DataFrame:")
json_df.show()

# Example operations on DataFrame
print("Schema of CSV DataFrame:")
csv_df.printSchema()

print("Count of rows in JSON DataFrame:")
print(json_df.count())

print("First row of CSV DataFrame:")
print(csv_df.first())

# Select and show specific columns
csv_df.select("name", "age").show()

# Filter rows where age is greater than 30
csv_df.filter(csv_df.age > 30).show()

# Group by age and count the number of occurrences
csv_df.groupBy("age").count().show()

# Order by age in descending order
csv_df.orderBy(csv_df.age.desc()).show()

# Stop the SparkSession
spark.stop()
