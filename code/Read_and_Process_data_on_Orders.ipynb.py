# Databricks notebook source
# MAGIC %md
# MAGIC ### Read and Process Orders

# COMMAND ----------

from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("Read_and_Process_Orders").getOrCreate()

# COMMAND ----------

spark

# COMMAND ----------

dataframe=spark.read.format("csv").option("inferschema","true").option("header","true").load("/Volumes/workspace/default/orders_hdfs_files/orders_500MB.csv")

# COMMAND ----------

dataframe.show()

# COMMAND ----------

dataframe.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Selection and Filtering

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

#  Retrieve all records where the order status is exactly "Pending".
dataframe.filter(col("status") =="Pending").show()

# COMMAND ----------

#  Find all orders where the total_amount exceeds $750
# dataframe.select("total_amount").filter( col("total_amount") > 750).show()
dataframe.select("total_amount","order_id").filter( col("total_amount") > 750).show()

# COMMAND ----------

#  Filter the dataset to show only orders placed by customer_id 48 that have been "Shipped".

# dataframe.filter(col("customer_id")==48).show()
dataframe.filter((col("customer_id")==48) & (col("status")=="Shipped")).show()

# COMMAND ----------

#  Select only the order_id and order_date columns for orders that are either "Cancelled" or "Pending".
# dataframe.select("order_date","order_id").show()
# dataframe.select("order_date","order_id").filter((col("status")=="Cancelled") | (col("status")=="Pending")).show()
# dataframe.select("order_date","order_id").filter(col("status").isin(["pending","Cancelled"])).show();
dataframe.select("order_date","order_id").filter("status in ('pending','Cancelled')").show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Aggregations and Grouping

# COMMAND ----------

# Calculate the total number of orders associated with each status category.
# dataframe.count()
count_result=dataframe.groupBy("status").count()

# COMMAND ----------

# count_result.show()
# count_result.orderBy("status").show()
count_result.orderBy(col("status").desc()).show()

# COMMAND ----------

# Find the average total_amount for orders that were "Cancelled"
# dataframe.filter(col("status")=="Cancelled").show()
# dataframe.filter(col("status")=="Cancelled").select(avg("total_amount")).show()
dataframe.filter(col("status")=="Cancelled").agg(avg("total_amount")).show()

# COMMAND ----------

# Identify the top 10 customer_ids who have the highest total spending (sum of total_amount)
# dataframe.groupBy("customer_id").agg(sum("total_amount")).orderBy(col("sum(total_amount)").desc()).show(10)
dataframe.groupBy("customer_id").agg(sum("total_amount")).orderBy(col("sum(total_amount)").desc()).limit(10).show()

# COMMAND ----------

# Determine the maximum and minimum total_amount across the entire dataset.
dataframe.select(min("total_amount"),max("total_amount")).show()

# COMMAND ----------

# Count how many unique customer_ids exist in the file.
dataframe.select(countDistinct("customer_id")).show()
# count_data=dataframe.select("customer_id").distinct().count()

# COMMAND ----------

count_data

# COMMAND ----------

# MAGIC %md
# MAGIC #### Date Manipulations

# COMMAND ----------

# Extract the year and month from order_date and create two new respective columns (order_year, order_month).
# dataframe.withColumn("order_year",year("order_date")).withColumn("order_month",month("order_date")).show()
dataframe.withColumn("order_year",year("order_date")).withColumn("order_month",month("order_date")).withColumn("day_of_year",dayofyear("order_date")).show()

# COMMAND ----------

# DBTITLE 1,Cell 23
# Calculate the total revenue (total_amount) generated in each distinct month.
# dataframe.withColumn("month",month("order_date")).distinct().groupBy(col("month")).agg(sum("total_amount")).show()

dataframe.withColumn("month",month("order_date")).distinct().groupBy(col("month")).agg(sum("total_amount")).orderBy("month").show()

# COMMAND ----------

# DBTITLE 1,Cell 24
# Find which day of the week experiences the highest volume of order placements.
# dataframe.withColumn("day_of_week", dayofweek("order_date")) \
#     .filter(col("status") == "Delivered") \
#     .groupBy("day_of_week") \
#     .count() \
#     .orderBy(col("count").desc()) \
#     .show()


dataframe.withColumn("day_of_week", dayofweek("order_date")) \
    .groupBy("day_of_week") \
    .count() \
    .orderBy(col("count").desc()) \
    .show()
dataframe.withColumn("day_of_week", dayofweek("order_date")) \
    .groupBy("day_of_week") \
    .count() \
    .orderBy(col("count").desc()) \
    .limit(1)\
    .show()

# COMMAND ----------

# Filter for all orders placed strictly between "2024-03-01" and "2024-06-30".
dataframe.filter((col("order_date")> "2024-03-01" ) & (col("order_date")< "2024-06-30")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transformations and Conditional Logic

# COMMAND ----------

# Create a new column named discounted_amount that applies a 12% discount to the total_amount
dataframe.withColumn("discounted_amount",(col("total_amount")*12/100)).show()

# COMMAND ----------

# Convert all string values in the status column to uppercase.
# dataframe.withColumn("upper_case", upper("status")).show()

# Approach 1: Using upper() with col()
dataframe.withColumn("upper_case", upper(col("status"))).show()

# Approach 2: Using upper() with dataframe column reference
dataframe.withColumn("upper_case", upper(dataframe.status)).show()

# Approach 3: Using selectExpr with SQL upper() function
dataframe.selectExpr("*", "upper(status) as upper_case").show()

# Approach 4: Using withColumn + expr()
dataframe.withColumn("upper_case", expr("upper(status)")).show()

# Approach 5: Using SQL query directly
dataframe.createOrReplaceTempView("orders")
spark.sql("SELECT *, upper(status) as upper_case FROM orders").show()

# Approach 6: Using a UDF
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
upper_udf = udf(lambda x: x.upper() if x else x, StringType())
dataframe.withColumn("upper_case", upper_udf("status")).show()

# COMMAND ----------

# Use conditional logic to create a new column called order_priority: mark as "High" if total_amount is greater than 800, "Medium" if between 400 and 800, and "Low" for anything below 400.

dataframe.withColumn("order_priority",
         when(col("total_amount") > 800,"High")\
         .when((col("total_amount")> 400) & (col("total_amount")<800),"Medium")\
         .otherwise("Low")).show()


# COMMAND ----------

# Cast the total_amount column from a float/double to an integer type, dropping the decimal values.
dataframe.withColumn("total_amount_as_integer",col("total_amount").cast("integer")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Window Functions

# COMMAND ----------

# DBTITLE 1,Cell 32
# Assign a dense rank to customers based on their overall total spending.

# Rank customers by their total spending using window functions
from pyspark.sql.window import Window

# First, calculate total spending per customer and add it as a column
df_with_total = dataframe.withColumn(
    "customer_total_spending",
    sum("total_amount").over(Window.partitionBy("customer_id"))
)

# Now create window function ordering by the calculated total
window_func = Window.orderBy(col("customer_total_spending").desc())

# Apply dense_rank
df_with_total.withColumn("dense_rank", dense_rank().over(window_func)).display()

# COMMAND ----------


# Now create window function ordering by the calculated total
window_func = Window.orderBy( sum("total_amount").over(Window.partitionBy("customer_id")) .desc())

# Apply dense_rank
df_with_total.withColumn("dense_rank", dense_rank().over(window_func)).display()

# COMMAND ----------

# DBTITLE 1,Cell 34
# Calculate a running total of the total_amount over time, ordered by order_date.
from pyspark.sql.window import Window

windows_func = Window.orderBy("order_date")
dataframe.withColumn("running_total", sum("total_amount").over(windows_func)).display()

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

# For each customer_id, find the time difference in days between their current order and their previous order.
window_func=Window.partitionBy("customer_id")

dataframe.withColumn("date_diff",datediff(max("order_date").over(window_func),min("order_date").over(window_func))).show()

# COMMAND ----------

#  Partition the data by status and find the top 3 highest total_amount orders within each status category.

windows_func=Window.partitionBy("status").orderBy(col("total_amount").desc())
dataframe.withColumn("dense_rank",dense_rank().over(windows_func)).filter(col("dense_rank") <= 3).show()