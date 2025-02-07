from pyspark.sql.functions import col, lit, when
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName('Dummy') \
    .getOrCreate()

df = spark.read.csv('emp_table.csv', header=True, inferSchema=True)

df2 = df.withColumn('Remark',
                    when(col('sal') > 3000, 'Highly Paid')
                    .when((col('sal') < 3000) & (col('sal') > 1000), 'Okayish')
                    .otherwise('nothing'))

df2.show()
