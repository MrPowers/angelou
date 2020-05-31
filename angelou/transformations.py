import pyspark.sql.functions as F
import quinn

def with_greeting(df):
    return df.withColumn("greeting", F.lit("hello!"))

def with_clean_first_name(df):
    return df.withColumn(
        "clean_first_name",
        quinn.remove_non_word_characters(F.col("first_name"))
    )

