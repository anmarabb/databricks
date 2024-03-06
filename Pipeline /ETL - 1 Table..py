# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, split, current_timestamp
from pyspark.sql.types import StringType, ArrayType, StructType, StructField
import re

# Create Spark Session
spark = SparkSession.builder.getOrCreate()

# Database configuration
driver = "org.postgresql.Driver"
database_host = "production-floranow-erp.cluster-ro-c47qrxmyb1ht.eu-central-1.rds.amazonaws.com"
database_port = "5432"
database_name = "floranow_erp_db"
user = "read_only"
password = "dENW5_J9DXcS@7"  # Reminder: Be cautious with passwords in code
url = f"jdbc:postgresql://{database_host}:{database_port}/{database_name}"


# Processing function
def process_table(table_name, custom_schema=""):
    try:
        print(f"Table {table_name} sync started")

        # Load data from the remote table
        jdbc_options = {
            "driver": driver,
            "url": url,
            "dbtable": table_name,
            "user": user,
            "password": password
        }

        if custom_schema:
            jdbc_options["customSchema"] = custom_schema

        remote_table = spark.read.format("jdbc").options(**jdbc_options).load()

        # Sanitize column names
        for column_name in remote_table.columns:
            sanitized_column_name = sanitize_column_name(column_name)
            remote_table = remote_table.withColumnRenamed(column_name, sanitized_column_name)

        # Replace 'public' schema with 'fn_sources.scr_fn_erp.' in the target table name
        target_table = table_name.replace('public.', 'fn_sources.scr_fn_erp.')

        # Specific transformations for 'line_items'
        if table_name == 'public.line_items':
            categorization_schema = ArrayType(StructType([StructField("permalink", StringType())]))
            remote_table = remote_table.withColumn("permalink", from_json(col("categorization"), categorization_schema).getItem(0)["permalink"])
            remote_table = remote_table.withColumn("categories", split(col("permalink"), "/"))
            num_categories = len(remote_table.select("categories").first()[0])
            for i in range(1, num_categories):
                remote_table = remote_table.withColumn(f"category{i}", remote_table["categories"].getItem(i))

        # Common transformations for both tables (if applicable)
        remote_table = remote_table.withColumn("ingestion_timestamp", current_timestamp())

        # Write data to Delta Lake
        remote_table.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(target_table)

        print(f"Table {target_table} has been copied to Delta Lake.")

    except Exception as e:
        print(f"An error occurred with table {table_name}: {e}")

# Process 'line_items' table with custom schema
line_items_custom_schema = "unit_fob_price DOUBLE, unit_landed_cost DOUBLE, exchange_rate DOUBLE, total_tax DOUBLE, total_price_include_tax DOUBLE, total_price_without_tax DOUBLE, unit_price DOUBLE, calculated_price DOUBLE, price_margin DOUBLE, unit_additional_cost DOUBLE, unit_shipment_cost DOUBLE, calculated_unit_price DOUBLE, unit_tax DOUBLE, packing_list_fob_price DOUBLE"
process_table('public.line_items', custom_schema=line_items_custom_schema)



# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, split, current_timestamp
from pyspark.sql.types import StringType, ArrayType, StructType, StructField
import re

# Create Spark Session
spark = SparkSession.builder.getOrCreate()

# Database configuration
driver = "org.postgresql.Driver"
database_host = "production-floranow-erp.cluster-ro-c47qrxmyb1ht.eu-central-1.rds.amazonaws.com"
database_port = "5432"
database_name = "floranow_erp_db"
user = "read_only"
password = "dENW5_J9DXcS@7"  # Reminder: Be cautious with passwords in code
url = f"jdbc:postgresql://{database_host}:{database_port}/{database_name}"


# Processing function
def process_table(table_name, custom_schema=""):
    try:
        print(f"Table {table_name} sync started")

        # Load data from the remote table
        jdbc_options = {
            "driver": driver,
            "url": url,
            "dbtable": table_name,
            "user": user,
            "password": password
        }

        if custom_schema:
            jdbc_options["customSchema"] = custom_schema

        remote_table = spark.read.format("jdbc").options(**jdbc_options).load()

        # Sanitize column names
        for column_name in remote_table.columns:
            sanitized_column_name = sanitize_column_name(column_name)
            remote_table = remote_table.withColumnRenamed(column_name, sanitized_column_name)

        # Replace 'public' schema with 'fn_sources.scr_fn_erp.' in the target table name
        target_table = table_name.replace('public.', 'fn_sources.scr_fn_erp.')

        # Specific transformations for 'line_items'
        if table_name == 'public.line_items':
            categorization_schema = ArrayType(StructType([StructField("permalink", StringType())]))
            remote_table = remote_table.withColumn("permalink", from_json(col("categorization"), categorization_schema).getItem(0)["permalink"])
            remote_table = remote_table.withColumn("categories", split(col("permalink"), "/"))
            num_categories = len(remote_table.select("categories").first()[0])
            for i in range(1, num_categories):
                remote_table = remote_table.withColumn(f"category{i}", remote_table["categories"].getItem(i))

        # Common transformations for both tables (if applicable)
        remote_table = remote_table.withColumn("ingestion_timestamp", current_timestamp())

        # Write data to Delta Lake
        remote_table.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(target_table)

        print(f"Table {target_table} has been copied to Delta Lake.")

    except Exception as e:
        print(f"An error occurred with table {table_name}: {e}")

process_table('public.payment_transactions')





# COMMAND ----------


