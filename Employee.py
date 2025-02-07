from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, year, to_date, expr

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("WasabiExample") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")  \
    .config("spark.hadoop.fs.native.disable.cache", "true") \
    .config("spark.executor.memory", "4g") \
    .master("spark://192.168.1.12:7077") \
    .config("spark.executor.cores", "4") \
    .getOrCreate()
    # .config("spark.driver.memory", "4g") \
    # .config("spark.driver.cores", "4") \
    # .config("spark.hadoop.fs.s3a.committer.name", "directory") \
    # .config("spark.sql.parquet.output.committer.class", "org.apache.hadoop.fs.s3a.commit.S3AOutputCommitter") \
    # .config("spark.sql.sources.commitProtocolClass", "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol") \
    # .config("spark.hadoop.fs.s3a.committer.staging.conflict-mode", "fail") \
    # .config("spark.hadoop.fs.s3a.committer.staging.tmp.path", "s3a://outputs/tmp/") \


#file_path = "shared/s.csv"  # Replace with the actual file path
#df = spark.read.csv(file_path, header=True, inferSchema=True)
print('hi')
# Replace 'your-bucket-name' and 'your-file-path'
df = spark.read.csv("s3a://sparker/employee_data.csv", header=True, inferSchema=True)
rdd = df.rdd
print(rdd.take(5))

df.show()

# # 1. Transformation: Add a column for bonus (10% of salary + commission)
# df = df.withColumn("bonus", (col("salary") * 0.1) + col("comm"))

# # 2. Transformation: Filter employees with salaries greater than 50,000
# df = df.filter(col("salary") > 50000)

# # 3. Transformation: Add a column for the year of hire
# df = df.withColumn("hire_year", year(to_date(col("hiredate"), "yyyy-MM-dd")))

# # 4. Transformation: Group by job and calculate average salary and total bonus
# df_grouped = df.groupBy("job").agg(
#     expr("avg(salary)").alias("avg_salary"),
#     expr("sum(bonus)").alias("total_bonus")
# )

# # 5. Transformation: Replace null commission values with 0
# df = df.withColumn("comm", when(col("comm").isNull(), lit(0)).otherwise(col("comm")))

# # 6. Transformation: Sort employees by salary in descending order
# df_sorted = df.orderBy(col("salary").desc())

# # 7. Transformation: Add a column indicating if the employee is eligible for a promotion
# df = df.withColumn("promotion_eligible", when(col("salary") > 75000, lit("Yes")).otherwise(lit("No")))

# # Show results (this action will trigger parallel execution)
# df.show(10)
# df_grouped.show()
# print('hello')
# # Save the transformed data for future use
# output_path = "s3a://outputs/silly"  # Replace with your desired output path
# df.write.mode("overwrite").csv(output_path + "/transformed_employee_data")
# #df.repartition(1).write.format("csv").save(output_path)
# df_grouped.write.mode("overwrite").csv(output_path + "/grouped_employee_data")
# print('done')
# # Stop the Spark session
# #spark.stop()
