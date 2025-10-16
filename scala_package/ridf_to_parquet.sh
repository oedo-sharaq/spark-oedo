#!/bin/bash

# RIDF to Parquet Converter Script
# Usage: ./ridf_to_parquet.sh <input_ridf_file> <output_parquet_file> [max_blocks]

if [ $# -lt 2 ]; then
    echo "Usage: $0 <input_ridf_file> <output_parquet_file> [max_blocks]"
    echo "Example: $0 /path/to/input.ridf /path/to/output.parquet"
    echo "Example: $0 /path/to/input.ridf /path/to/output.parquet 10000"
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

echo "Converting RIDF file to Parquet..."
echo "Input:  $INPUT_FILE"
echo "Output: $OUTPUT_FILE"
if [ -n "$MAX_BLOCKS" ]; then
    echo "Max blocks: $MAX_BLOCKS"
fi

# Run the Spark application
if [ -n "$MAX_BLOCKS" ]; then
    spark-submit --class RidfToParquetSimple \
                 --master local[*] \
                 --driver-memory 12g \
                 --executor-memory 8g \
                 --conf spark.sql.execution.arrow.maxRecordsPerBatch=1000 \
                 --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
                 "$JAR_PATH" \
                 "$INPUT_FILE" \
                 "$OUTPUT_FILE" \
                 "$MAX_BLOCKS"
else
    spark-submit --class RidfToParquetSimple \
                 --master local[*] \
                 --driver-memory 12g \
                 --executor-memory 8g \
                 --conf spark.sql.execution.arrow.maxRecordsPerBatch=1000 \
                 --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
                 "$JAR_PATH" \
                 "$INPUT_FILE" \
                 "$OUTPUT_FILE"
fi

if [ $? -eq 0 ]; then
    echo "Conversion completed successfully!"
    echo "Output files created in: $OUTPUT_FILE"
else
    echo "Conversion failed!"
    exit 1
fi