from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, max, min, count, when

# Initialize Spark session
spark = SparkSession.builder.appName("ComprehensiveSalesDataProcessing").getOrCreate()

# Load ingested sales data from HDFS
hdfs_path = "hdfs://localhost:50000/sales"
sales_data = spark.read.csv(hdfs_path, header=True, inferSchema=True)

# Data Processing Operations

# 1. Data Filtering: Filter out rows where sales amount is less than 100
filtered_data = sales_data.filter(col("sales_amount") >= 100)

# 2. Handle Missing Data: Fill missing values in 'sales_amount' with the average sales
avg_sales = sales_data.agg(avg("sales_amount")).first()[0]
cleaned_data = filtered_data.fillna({"sales_amount": avg_sales})

# 3. Adding a New Column: Add a column that categorizes sales as 'High' or 'Low'
categorized_data = cleaned_data.withColumn(
    "sales_category",
    when(col("sales_amount") > 500, "High").otherwise("Low")
)

# 4. Aggregations:
#    a. Total Sales Amount by Region
total_sales_by_region = categorized_data.groupBy("region").agg(
    sum("sales_amount").alias("total_sales")
)

#    b. Average Sales Amount by Product
avg_sales_by_product = categorized_data.groupBy("product").agg(
    avg("sales_amount").alias("average_sales")
)

#    c. Maximum and Minimum Sales by Salesperson
max_min_sales_by_salesperson = categorized_data.groupBy("salesperson").agg(
    max("sales_amount").alias("max_sales"),
    min("sales_amount").alias("min_sales")
)

# 5. Sorting: Sort regions by total sales in descending order
sorted_sales_by_region = total_sales_by_region.orderBy(col("total_sales").desc())

# 6. Advanced Filtering: Filter regions with total sales greater than 10,000
high_sales_regions = sorted_sales_by_region.filter(col("total_sales") > 10000)

# 7. Count Distinct Values: Count unique products sold
unique_products_count = categorized_data.agg(count("product").alias("unique_products"))

# Save all processed results to explicit HDFS paths
total_sales_by_region.write.mode("overwrite").csv("hdfs://localhost:50000/processed_sales_data/total_sales_by_region")
avg_sales_by_product.write.mode("overwrite").csv("hdfs://localhost:50000/processed_sales_data/avg_sales_by_product")
max_min_sales_by_salesperson.write.mode("overwrite").csv("hdfs://localhost:50000/processed_sales_data/max_min_sales_by_salesperson")
high_sales_regions.write.mode("overwrite").csv("hdfs://localhost:50000/processed_sales_data/high_sales_regions")
unique_products_count.write.mode("overwrite").csv("hdfs://localhost:50000/processed_sales_data/unique_products_count")

print("Sales data successfully processed and saved to explicit output paths.")

# Stop the Spark session
spark.stop()
