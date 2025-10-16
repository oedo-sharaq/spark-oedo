#!/usr/bin/env python3
"""
RIDF Processor - Python version
Processes RIDF files and extracts decoded events directly
"""

import sys
import os
import argparse
from pathlib import Path
from pyspark.sql import SparkSession
from spark_oedo_utils import find_jar_path, print_jar_info

def create_spark_session(app_name="RIDF Processor"):
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

def call_scala_processor(input_file, output_file, max_blocks=None):
    """Call the Scala RidfProcessor application"""
    
    script_dir = Path(__file__).parent.parent
    jar_path = find_jar_path(script_dir)
    
    # Build spark-submit command with memory optimizations
    cmd = [
        "spark-submit",
        "--class", "RidfProcessor", 
        "--master", "local[*]",
        "--driver-memory", "12g",
        "--executor-memory", "12g", 
        "--conf", "spark.driver.maxResultSize=8g",
        "--conf", "spark.driver.memoryFraction=0.8",
        "--conf", "spark.storage.memoryFraction=0.6",
        "--conf", "spark.sql.adaptive.enabled=false",
        "--conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer",
        "--conf", "spark.kryoserializer.buffer.max=256m",
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
        description="Process RIDF files and extract decoded events directly",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s input.ridf output.parquet
  %(prog)s input.ridf output.parquet --max-blocks 10000
  
Output format (decoded events):
  - run_number: Integer
  - event_number: Integer  
  - timestamp: Long
  - fp: Integer (focal plane)
  - dev: Integer (device)
  - det: Integer (detector)
  - mod: Integer (module)
  - data: Binary array
        """)
    
    parser.add_argument("input_file", help="Input RIDF file path")
    parser.add_argument("output_file", help="Output Parquet file path")
    parser.add_argument("--max-blocks", type=int, help="Maximum number of blocks to process")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    
    args = parser.parse_args()
    
    # Validate input file
    if not os.path.exists(args.input_file):
        print(f"Error: Input file '{args.input_file}' does not exist")
        sys.exit(1)
    
    print("RIDF Event Processor")
    print("=" * 50)
    print(f"Input:  {args.input_file}")
    print(f"Output: {args.output_file}")
    if args.max_blocks:
        print(f"Max blocks: {args.max_blocks:,}")
    print(f"Mode: Event extraction and decoding")
    print()
    
    try:
        success = call_scala_processor(args.input_file, args.output_file, args.max_blocks)
        
        if success:
            print("\n✓ Processing completed successfully!")
            print(f"Output created: {args.output_file}")
            print("\nThe output contains decoded events with columns:")
            print("  run_number, event_number, timestamp, fp, dev, det, mod, data")
            print("\nYou can read the result in PySpark with:")
            print(f"  df = spark.read.parquet('{args.output_file}')")
            print("  df.printSchema()")
            print("  df.show()")
            print("  df.groupBy('det', 'dev').count().show()  # Show detector/device counts")
        else:
            print("\n✗ Processing failed!")
            sys.exit(1)
            
    except Exception as e:
        print(f"\n✗ Error during processing: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()