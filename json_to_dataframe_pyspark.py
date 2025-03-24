from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, expr
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
import json

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("JSON to DataFrame Converter") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "100") \
    .getOrCreate()

# First read JSON into Python variable
with open('template_odin_response.json', 'r') as file:
    json_data = json.load(file)

# Extract only the results array
records = json_data['results']

# Convert Python list to Spark DataFrame
df = spark.createDataFrame(records)

# Define schema for nested JSON columns
# Note: You might need to adjust these schemas based on your actual JSON structure
incident_schema = StructType([
    # Add your incident fields here
    # Example:
    # StructField("id", StringType(), True),
    # StructField("type", StringType(), True)
])

outagearea_schema = StructType([
    # Add your outagearea fields here
])

names_schema = StructType([
    # Add your names fields here
])

incident_location_schema = StructType([
    # Add your incident_location fields here
])

# Dictionary of column names and their corresponding schemas
json_columns = {
    'incident': incident_schema,
    'outagearea': outagearea_schema,
    'names': names_schema,
    'incident_location': incident_location_schema
}

# Parse nested JSON columns
for col_name, schema in json_columns.items():
    if col_name in df.columns:
        # Parse the JSON string in the column
        df = df.withColumn(
            col_name,
            from_json(col(col_name), schema)
        )
        
        # Flatten the struct column
        nested_cols = df.select(f"{col_name}.*").columns
        for nested_col in nested_cols:
            df = df.withColumn(
                f"{col_name}_{nested_col}",
                col(f"{col_name}.{nested_col}")
            )
        
        # Drop the original column
        df = df.drop(col_name)

# Optimize the DataFrame
df = df.repartition(10)  # Adjust number based on your data size
df.cache()

# Write to CSV
df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("output_spark")

# Optional: Show the schema and sample data
print("\nDataFrame Schema:")
df.printSchema()

print("\nSample Data:")
df.show(5, truncate=False)

# Add error handling
try:
    # Your existing code here
    pass
except Exception as e:
    print(f"Error processing file: {str(e)}")
finally:
    spark.stop()
