#!/usr/bin/env python3
"""
RIDF Parquet Processor

This script processes RIDF block data from a parquet file and outputs decoded events
to another parquet file using the decodeBlock function.

Usage:
    python ridf_parquet_processor.py input.parquet output.parquet

Requirements:
- PySpark with the spark-oedo-package JAR file in classpath
- Input parquet file containing RIDF block data with columns: block_id, block_data
"""

import sys
import argparse
from pyspark.sql import SparkSession
from sparkOEDOModules.decoders.ridfBlockDecoder import decodeBlock


def create_spark_session(jar_path=None):
    """Create Spark session with RIDF decoder JAR loaded."""
    builder = SparkSession.builder.appName("RIDF Parquet Processor")
    
    if jar_path:
        builder = builder.config("spark.jars", jar_path)
    
    # Try to find the JAR in common locations if not specified
    if not jar_path:
        import os
        possible_jar_paths = [
            "./target/scala-2.13/spark-oedo-package_2.13-1.0.jar",
            "../scala_package/target/scala-2.13/spark-oedo-package_2.13-1.0.jar",
            "../../scala_package/target/scala-2.13/spark-oedo-package_2.13-1.0.jar"
        ]
        
        for path in possible_jar_paths:
            if os.path.exists(path):
                builder = builder.config("spark.jars", path)
                print(f"[INFO] Using JAR: {path}")
                break
        else:
            print("[WARNING] JAR file not found in common locations. Make sure spark-oedo-package JAR is in classpath.")
    
    return builder.getOrCreate()


def main():
    parser = argparse.ArgumentParser(description="Process RIDF parquet files and decode block data")
    parser.add_argument("input", help="Input parquet file path")
    parser.add_argument("output", help="Output parquet file path")
    parser.add_argument("--jar", help="Path to spark-oedo-package JAR file")
    parser.add_argument("--coalesce", type=int, default=1, help="Number of output partitions (default: 1)")
    
    args = parser.parse_args()
    
    # Create Spark session
    spark = create_spark_session(args.jar)
    
    try:
        print(f"[INFO] Reading input parquet file: {args.input}")
        
        # Read input parquet file
        df = spark.read.parquet(args.input)
        
        # Show input schema and count
        print("[INFO] Input schema:")
        df.printSchema()
        print(f"[INFO] Input record count: {df.count()}")
        
        # Check if required columns exist
        required_columns = ["block_data"]  # block_id is optional
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        # Decode RIDF blocks
        print("[INFO] Decoding RIDF blocks...")
        decoded_df = decodeBlock(spark, df)
        
        # Show output schema and count
        print("[INFO] Output schema:")
        decoded_df.printSchema()
        decoded_count = decoded_df.count()
        print(f"[INFO] Output record count: {decoded_count}")
        
        # Write output parquet file
        print(f"[INFO] Writing output parquet file: {args.output}")
        decoded_df.write.mode("overwrite").parquet(args.output)
        
        print(f"[SUCCESS] Processing completed successfully!")
        print(f"[SUCCESS] Processed {decoded_count} events")
        
    except Exception as e:
        print(f"[ERROR] Processing failed: {str(e)}")
        sys.exit(1)
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()