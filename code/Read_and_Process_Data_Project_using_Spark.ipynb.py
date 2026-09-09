# Databricks notebook source
# MAGIC %md
# MAGIC ## Read and Process Data

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Read_and_Process_Data_Project").getOrCreate()

# COMMAND ----------

spark

# COMMAND ----------

# dataframe=spark.read.format("csv").option("header","true").option("inferschema","true").load("/Volumes/workspace/default/hdfs_files/customers_500MB.csv");

# COMMAND ----------

dataframe=spark.read.format("csv").option("header","true").load("/Volumes/workspace/default/hdfs_files/customers_500MB.csv");

# COMMAND ----------

dataframe.printSchema()

# COMMAND ----------

dataframe.show(5)

# COMMAND ----------

from pyspark.sql.functions import *


# COMMAND ----------

# MAGIC %md
# MAGIC #### converting data type of columns

# COMMAND ----------

dataframe=dataframe.withColumn("registration_date",to_date(col("registration_date"),'yyyy-MM-dd')).withColumn("is_active",col("is_active").cast('boolean'))

# COMMAND ----------

dataframe.printSchema()

# COMMAND ----------

dataframe=dataframe.fillna({'city': 'unknown', 'state': 'unknown', 'country': 'unknown'})

# COMMAND ----------

dataframe.tail(5)

# COMMAND ----------

df=dataframe.withColumn("registration_year",year(col("registration_date"))).withColumn("registration_month",month(col("registration_date"))).withColumn("registration_day",dayofmonth(col("registration_date"))).withColumn("registration_hour",hour(col("registration_date"))).withColumn("registration_minute",minute(col("registration_date")))

# COMMAND ----------

df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC #### working with select() and countDistinct()

# COMMAND ----------

unique_state_data=dataframe.select(countDistinct("state")).collect()

# COMMAND ----------

unique_city_data=dataframe.select(countDistinct("city")).collect()

# COMMAND ----------

print(unique_state_data)
print(unique_city_data)

# COMMAND ----------

unique_city_data[0][0]

# COMMAND ----------

# MAGIC %md
# MAGIC #### working with groupBy(),orderBy() and count()

# COMMAND ----------

dataframe.groupBy("city").count().show()

# COMMAND ----------

# ascending order
dataframe.groupBy("city").count().orderBy("count").show()


# COMMAND ----------

# descending order
dataframe.groupBy("city").count().orderBy(col("count").desc()).show()

# COMMAND ----------

# dataframe.groupBy("state").count().orderBy(col("state").desc()).show()
dataframe.groupBy("state").count().orderBy(col("state")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### working pivot table  count of active user and inactive users

# COMMAND ----------

dataframe.groupBy("is_active").count().show()

# COMMAND ----------

dataframe.groupBy("city").pivot("is_active").count().show()

# COMMAND ----------

dataframe.groupBy("state").pivot("is_active").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### working with window function

# COMMAND ----------

from pyspark.sql.window import Window
# window_spec=Window.partitionBy("registration_date").orderBy(col("registration_date").desc())
window_spec=Window.partitionBy("state").orderBy(col("registration_date").desc())


# COMMAND ----------

window_result=dataframe.withColumn("rank()",rank().over(window_spec)).withColumn("dense_rank()",dense_rank().over(window_spec)).withColumn("row_number()",row_number().over(window_spec))

# COMMAND ----------

window_result.select("name","city","state","rank()","dense_rank()","row_number()").show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### working with filter()

# COMMAND ----------

dataframe.filter(col("registration_date")>= lit('2023-10-01')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### working with aggrigate function agg()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### oldest and newest customer per city

# COMMAND ----------

old_and_new_customers=dataframe.groupBy("state").agg(min("registration_date").alias("old_customers"),max("registration_date").alias("new_customers"))

# COMMAND ----------

old_and_new_customers.show()
old_and_new_customers.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #### error case

# COMMAND ----------

# DBTITLE 1,Cell 40
# output_table = "processed_customers_result"

# dataframe.write.mode("overwrite").saveAsTable(output_table)
output_path="/Workspace/Users/manubolukishore@gmail.com/Spark_Projects/processed_customers_Result"
dataframe.write.mode('overwrite').parquet(output_path)

#  throwing error ....need to troubleshoot

# COMMAND ----------

# MAGIC %md
# MAGIC ####  wokring with joining and analyzing customers and orders

# COMMAND ----------

# create dataframe from orders csv file
customer_orders_dataframe=spark.read.format("csv").option("header","true").option("inferSchema","true").load("/Volumes/workspace/default/orders_hdfs_files/orders_500MB.csv")

# COMMAND ----------

customer_orders_dataframe.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### inner join

# COMMAND ----------

inner_join_df=dataframe.join(customer_orders_dataframe,dataframe.customer_id == customer_orders_dataframe.customer_id,"inner")

# COMMAND ----------

inner_join_df.show()

# COMMAND ----------

inner_join_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### outer join

# COMMAND ----------

outer_join_df=dataframe.join(customer_orders_dataframe,dataframe.customer_id == customer_orders_dataframe.customer_id,"outer")

# COMMAND ----------

outer_join_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### left outer join

# COMMAND ----------

left_outer_join_df=dataframe.join(customer_orders_dataframe,dataframe.customer_id == customer_orders_dataframe.customer_id,"left_outer")

# COMMAND ----------

left_outer_join_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### right outer join

# COMMAND ----------

right_outer_join_df=dataframe.join(customer_orders_dataframe,dataframe.customer_id == customer_orders_dataframe.customer_id,"right_outer")

# COMMAND ----------

right_outer_join_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #####  counting inner join result

# COMMAND ----------

inner_join_df.count()

# COMMAND ----------

inner_join_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### finding Total Orders based on customer_id

# COMMAND ----------

inner_join_df.groupBy("customer_id").count().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Above code throws [AMBIGUOUS_REFERENCE] Reference `customer_id` is ambiguous, could be: [`customer_id`, `customer_id`]. SQLSTATE: ##### 42704
# MAGIC ##### == DataFrame ==
# MAGIC ##### "col" was called from , line 1 in cell [92]
# MAGIC ##### solution: use common column name(PK) in join() function

# COMMAND ----------

inner_join_df=dataframe.join(customer_orders_dataframe,"customer_id","inner")

# COMMAND ----------

inner_join_df.groupBy("customer_id").count().display()

# COMMAND ----------

inner_join_df.groupBy("customer_id").agg(sum("total_amount")).display()                     

# COMMAND ----------

# find premium(highest) customer total amount spend
inner_join_df.groupBy("customer_id").agg(sum("total_amount")).orderBy(col('sum(total_amount)').desc()).display()   

# COMMAND ----------

inner_join_df.groupBy("customer_id").agg(sum("total_amount")).orderBy(col('sum(total_amount)').desc()).show(10)   

# COMMAND ----------

# MAGIC %md
# MAGIC ##### find average spend per customer

# COMMAND ----------

inner_join_df.groupBy("customer_id").agg(avg("total_amount")).orderBy(col('avg(total_amount)').desc()).show(10)   

# COMMAND ----------

# MAGIC %md
# MAGIC ##### orderBy status

# COMMAND ----------

order_status_asce=inner_join_df.orderBy("status").groupBy("status").count()

# COMMAND ----------

order_status_asce.show()

# COMMAND ----------

inner_join_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### order by month

# COMMAND ----------

# DBTITLE 1,Cell 76
order_by_month_df=inner_join_df.withColumn("month",month("order_date")).groupBy(col("month")).count().orderBy(col("month"))
order_by_month_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### dense_rank for total spent based on Window function

# COMMAND ----------

# DBTITLE 1,Cell 78
from pyspark.sql.functions import *
windows_func=Window.orderBy(col("sum(total_amount)").desc())

custoomer_total_spend=inner_join_df.groupBy("customer_id").agg(sum("total_amount"))


reanked_customers=custoomer_total_spend.withColumn("dense_rank",dense_rank().over(windows_func))
reanked_customers.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### finding customers with High frequency but Low Total Spend

# COMMAND ----------

customer_count_df=inner_join_df.groupBy("customer_id").count()
total_spent=inner_join_df.groupBy("customer_id").agg(sum("total_amount")).orderBy(col('sum(total_amount)'))

customer_count_df.printSchema()
total_spent.printSchema()



# COMMAND ----------

customer_orders_vs_spend=customer_count_df.join(total_spent,"customer_id","inner")\
    .orderBy(col("count").desc(),col("sum(total_amount)"))

customer_orders_vs_spend.show()

# COMMAND ----------

# DBTITLE 1,Cell 82
output_path2="/Volumes/workspace/default/hdfs_files/joins_customers_and_orders_result"
inner_join_df.write.mode("overwrite").parquet(output_path2)
