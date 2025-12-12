#!/usr/bin/env python3
"""
StreamingV1 to Parquet Converter - Python version
Converts StreamingV1 data files to Parquet format with raw time-frame blocks
"""

import sys
import os
import argparse
from pathlib import Path
from pyspark.sql import SparkSession
from spark_oedo_utils import find_jar_path, print_jar_info

def create_spark_session(app_name="StreamingV1 to Parquet Converter"):
    """Create Spark session with appropriate configuration"""
    
    # Find the JAR file automatically
    script_dir = Path(__file__).parent.parent
    jar_path = find_jar_path(script_dir)
    print_jar_info(jar_path)
    
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", str(jar_path)) \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.driver.maxResultSize", "4g") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
        .master("local[*]") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def call_scala_converter(input_file, output_file, max_blocks=None):
    """Call the Scala StreamingV1ToParquet application via spark-submit"""

    script_dir = Path(__file__).parent.parent
    jar_path = find_jar_path(script_dir)

    # Build spark-submit command targeting StreamingV1ToParquet
    cmd = [
        "spark-submit",
        "--class", "StreamingV1ToParquet",
        "--master", "local[*]",
        "--driver-memory", "8g",
        "--executor-memory", "8g",
        "--conf", "spark.driver.maxResultSize=4g",
        "--conf", "spark.sql.execution.arrow.pyspark.enabled=false",
        str(jar_path),
        input_file,
        output_file
    ]

    if max_blocks is not None:
        cmd.append(str(max_blocks))

    print("Running:", " ".join(cmd))
    result = os.system(" ".join(cmd))

    return result == 0

def main():
    parser = argparse.ArgumentParser(
        description="Convert StreamingV1 data files to Parquet format with raw time-frame blocks",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s input.dat output.parquet
  %(prog)s input.dat output.parquet --max-blocks 10000
  
Output format:
  - block_id: Integer (0, 1, 2, ...)
  - block_data: Binary array (raw time-frame block bytes)
        """)

    parser.add_argument("input_file", help="Input file path")
    parser.add_argument("output_file", help="Output Parquet file path")
    parser.add_argument("--max-blocks", type=int, help="Maximum number of blocks to process")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")

    args = parser.parse_args()
    
    # Validate input file
    if not os.path.exists(args.input_file):
        print(f"Error: Input file '{args.input_file}' does not exist")
        sys.exit(1)
    
    print("StreamingV1 to Parquet Converter")
    print("=" * 50)
    print(f"Input:  {args.input_file}")
    print(f"Output: {args.output_file}")
    if args.max_blocks:
        print(f"Max blocks: {args.max_blocks:,}")
    print(f"Mode: Raw block data conversion")
    print()
    
    try:
        success = call_scala_converter(args.input_file, args.output_file, args.max_blocks)
        
        if success:
            print("\n✓ Conversion completed successfully!")
            print(f"Output files created: {args.output_file}")
            print("\nYou can read the result in PySpark with:")
            print(f"  df = spark.read.parquet('{args.output_file}')")
            print("  df.printSchema()")
            print("  df.show()")
        else:
            print("\n✗ Conversion failed!")
            sys.exit(1)
            
    except Exception as e:
        print(f"\n✗ Error during conversion: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()