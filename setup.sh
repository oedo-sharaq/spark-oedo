#!/bin/bash

# spark-oedo Setup Script
# Source this script to set up the environment for RIDF processing tools

# Get the directory where this script is located
export SPARK_OEDO_HOME="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Add bin directory to PATH if not already there
if [[ ":$PATH:" != *":$SPARK_OEDO_HOME/bin:"* ]]; then
    export PATH="$SPARK_OEDO_HOME/bin:$PATH"
fi

# Set PYTHONPATH to include the project modules
if [[ ":$PYTHONPATH:" != *":$SPARK_OEDO_HOME:"* ]]; then
    export PYTHONPATH="$SPARK_OEDO_HOME:$PYTHONPATH"
fi

# Function to build the Scala package
build_spark_oedo() {
    echo "Building spark-oedo Scala package..."
    cd "$SPARK_OEDO_HOME/scala_package" || return 1
    sbt package
    cd - > /dev/null || return 1
}

# Function to show available tools
spark_oedo_help() {
    echo "spark-oedo RIDF Processing Tools"
    echo "================================"
    echo ""
    echo "Available commands:"
    echo "  ridf_to_parquet.py   - Convert RIDF to Parquet (raw blocks)"
    echo "  ridf_process.py      - Process RIDF and extract events"
    echo "  build_spark_oedo     - Build the Scala package"
    echo ""
    echo "Environment:"
    echo "  SPARK_OEDO_HOME = $SPARK_OEDO_HOME"
    echo ""
    echo "Examples:"
    echo "  ridf_to_parquet.py input.ridf output_blocks.parquet"
    echo "  ridf_process.py input.ridf output_events.parquet"
    echo "  ridf_process.py input.ridf output.parquet --max-blocks 50000"
    echo ""
    echo "Memory considerations:"
    echo "  - Large files (>2GB): Ensure you have 16GB+ RAM available"
    echo "  - Very large files (>10GB): Consider splitting the input first"
    echo "  - Processing uses ~12GB driver memory by default"
    echo "  - Batch processing limits memory usage per batch"
    echo ""
    echo "For detailed help:"
    echo "  ridf_to_parquet.py --help"
    echo "  ridf_process.py --help"
}

# Check if JAR exists
JAR_PATH="$SPARK_OEDO_HOME/scala_package/target/scala-2.12/spark-oedo-package_2.12-1.0.jar"
if [ ! -f "$JAR_PATH" ]; then
    echo "⚠️  Scala JAR not found. You may need to build the project first."
    echo "   Run: build_spark_oedo"
fi

echo "✓ spark-oedo environment loaded"
echo "  Home: $SPARK_OEDO_HOME"
echo "  Run 'spark_oedo_help' for available commands"