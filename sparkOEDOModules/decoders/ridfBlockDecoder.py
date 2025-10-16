from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

def decodeBlock(spark: SparkSession, df: DataFrame, dataColName: str = "block_data") -> DataFrame:
    """Decode RIDF blocks in the given DataFrame.
    Args:
        spark: SparkSession instance with the RIDF decoder JAR loaded
        df: Input DataFrame containing RIDF block data
        dataColName: Name of the column containing binary RIDF data (default: "block_data")
    """
    # Register the UDFs directly using the JAR
    try:
        spark.sparkContext._jvm.oedo.udfs.decoders.RIDFBlockDecoder.registerUDF(spark._jsparkSession)
    except Exception as e:
        print(f"Warning: Could not register UDFs: {e}")
    
    # Extract run number using the registered UDF
    run = df.withColumn("run", F.expr(f"extract_run_number_sql({dataColName})")).select("run").collect()[0][0]
    df_result = (
        df.withColumn("decoded_block",F.expr(f"decode_ridf_block({dataColName})"))
            .select("*", F.explode("decoded_block"))
            .select("col.*")
            .select("event_number","timestamp",F.explode("segdata").alias("seg"))
            .select("event_number","timestamp","seg.*")
            .select(
                "event_number",
                "timestamp",
                F.col("fFP").alias("fp"),
                F.col("fDevice").alias("dev"),
                F.col("fDetector").alias("det"),
                F.col("fModule").alias("mod"),
                "data"
            )
            .withColumn("run",F.lit(run))
    )
    return df_result