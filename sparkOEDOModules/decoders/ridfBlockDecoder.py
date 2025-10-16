from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from ridf_functions import register_ridf_functions, extract_run_number

def decodeBlock(spark: SparkSession, df: DataFrame, dataColName: str = "block_data") -> DataFrame:
    """Decode RIDF blocks in the given DataFrame.
    Args:
        spark: SparkSession instance with the RIDF decoder JAR loaded
        df: Input DataFrame containing RIDF block data
        dataColName: Name of the column containing binary RIDF data (default: "block_data")
    """
    register_ridf_functions(spark)
    run = df.withColumn("run",extract_run_number(F.col(dataColName))).select("run").collect()[0][0]
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