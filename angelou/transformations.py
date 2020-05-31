import pyspark.sql.functions as F

def with_greeting(df):
    return df.withColumn("greeting", F.lit("hello!"))

