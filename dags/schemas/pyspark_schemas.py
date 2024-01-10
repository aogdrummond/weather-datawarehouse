from pyspark.sql.types import StructType, StructField, StringType, ArrayType

REQUEST_SCHEMA = ArrayType(StructType([
    StructField("method", StringType(), False),
    StructField("city", StringType(), False),
    StructField("result", StructType(), True)
]))