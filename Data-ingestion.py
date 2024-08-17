from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("SalesDataIngestion").getOrCreate()

# Define HDFS path
hdfs_path = "hdfs://localhost:50000/sales"

# Load CSV data from the local dataset path
local_dataset_path = "/divithraju/hadoop/salesdata/"
sales_data = spark.read.csv(local_dataset_path, header=True, inferSchema=True)

# Save the data to HDFS in CSV format
sales_data.write.mode("overwrite").csv(hdfs_path)

print("Sales data successfully ingested into HDFS at", hdfs_path)

# Stop the Spark session
spark.stop()
