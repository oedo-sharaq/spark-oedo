#!/bin/bash

# RIDF Processor Script - Processes RIDF and extracts decoded events directly
# Usage: ./ridf_process.sh <input_ridf_file> <output_parquet_file> [max_blocks]

if [ $# -lt 2 ]; then
    echo "Usage: $0 <input_ridf_file> <output_parquet_file> [max_blocks]"
    echo "Example: $0 /path/to/input.ridf /path/to/output.parquet"
    echo "Example: $0 /path/to/input.ridf /path/to/output.parquet 10000"
    echo ""
    echo "This processes RIDF files and extracts decoded events with structure:"
    echo "  - run_number: Int"
    echo "  - event_number: Int" 
    echo "  - timestamp: Long"
    echo "  - fp: Int"
    echo "  - dev: Int"
    echo "  - det: Int"
    echo "  - mod: Int"
    echo "  - data: Binary"
    exit 1
fi

INPUT_FILE="$1"
OUTPUT_FILE="$2"
MAX_BLOCKS="${3:-}"

# Check if input file exists
if [ ! -f "$INPUT_FILE" ]; then
    echo "Error: Input file '$INPUT_FILE' does not exist"
    exit 1
fi

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
JAR_PATH="$SCRIPT_DIR/target/scala-2.12/spark-oedo-package_2.12-1.0.jar"

# Check if JAR exists, if not try to build it
if [ ! -f "$JAR_PATH" ]; then
    echo "JAR not found at $JAR_PATH"
    echo "Building JAR..."
    cd "$SCRIPT_DIR"
    sbt package
    if [ $? -ne 0 ]; then
        echo "Failed to build JAR"
        exit 1
    fi
fi

echo "Processing RIDF file and extracting events..."
echo "Input:  $INPUT_FILE"
echo "Output: $OUTPUT_FILE"
if [ -n "$MAX_BLOCKS" ]; then
    echo "Max blocks: $MAX_BLOCKS"
fi
echo "Output will contain decoded event data (not raw blocks)"

# Run the RIDF processor application
if [ -n "$MAX_BLOCKS" ]; then
    spark-submit --class RidfProcessor \
                 --master local[*] \
                 --driver-memory 8g \
                 --executor-memory 8g \
                 --conf spark.driver.maxResultSize=4g \
                 --conf spark.sql.execution.arrow.pyspark.enabled=false \
                 "$JAR_PATH" \
                 "$INPUT_FILE" \
                 "$OUTPUT_FILE" \
                 "$MAX_BLOCKS"
else
    spark-submit --class RidfProcessor \
                 --master local[*] \
                 --driver-memory 8g \
                 --executor-memory 8g \
                 --conf spark.driver.maxResultSize=4g \
                 --conf spark.sql.execution.arrow.pyspark.enabled=false \
                 "$JAR_PATH" \
                 "$INPUT_FILE" \
                 "$OUTPUT_FILE"
fi

if [ $? -eq 0 ]; then
    echo "Processing completed successfully!"
    echo "Output created: $OUTPUT_FILE"
    echo ""
    echo "The output contains decoded events with columns:"
    echo "  run_number, event_number, timestamp, fp, dev, det, mod, data"
    echo ""
    echo "You can read it in PySpark with:"
    echo "  df = spark.read.parquet('$OUTPUT_FILE')"
else
    echo "Processing failed!"
    exit 1
fi