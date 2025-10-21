#!/bin/bash

# MIRA Decoder Usage Examples
# This script demonstrates how to use the MIRA decoder once SBT is installed

echo "MIRA Decoder - Scala Translation Usage Examples"
echo "==============================================="
echo ""

# Check if test.dat exists
if [ ! -f "../test.dat" ]; then
    echo "ERROR: test.dat not found in parent directory"
    echo "Please ensure you have a MIRA binary data file available"
    exit 1
fi

echo "Available test data: ../test.dat"
echo ""

echo "Usage examples (after installing SBT):"
echo ""

echo "1. Basic decoding with default channels (0,1,2,3,4):"
echo "   sbt \"runMain MiraDecoderMain ../test.dat\""
echo ""

echo "2. Specify output file:"
echo "   sbt \"runMain MiraDecoderMain ../test.dat decoded_output.parquet\""
echo ""

echo "3. Process specific channels only:"
echo "   sbt \"runMain MiraDecoderMain ../test.dat decoded_output.parquet 0 1 2\""
echo ""

echo "4. Create fat JAR for standalone use:"
echo "   sbt assembly"
echo "   java -jar target/scala-2.13/*.jar ../test.dat"
echo ""

echo "5. With custom JVM options for large files:"
echo "   java -Xmx8g -XX:+UseG1GC -jar target/scala-2.13/*.jar ../test.dat"
echo ""

echo "Build commands:"
echo "   sbt compile     # Compile the project"
echo "   sbt test        # Run tests"
echo "   sbt assembly    # Create fat JAR"
echo ""

echo "Installation of SBT (if needed):"
echo "   # Ubuntu/Debian:"
echo "   sudo apt-get install sbt"
echo ""
echo "   # Or download from https://www.scala-sbt.org/download.html"
echo ""

echo "Project structure:"
echo "   src/main/scala/"
echo "   ├── MiraDecoder.scala        # Core decoder and data structures"
echo "   ├── MiraArrowConverter.scala # Arrow table conversion"
echo "   ├── MiraDecoderMain.scala    # Main application"
echo "   ├── RidfProcessor.scala      # Original RIDF processor"
echo "   └── RidfToParquetSimple.scala # Original RIDF converter"
echo ""

echo "Dependencies managed by build.sbt:"
echo "   - Apache Arrow 14.0.0"
echo "   - Apache Parquet 1.13.1"
echo "   - Apache Spark 4.0.0"
echo "   - Hadoop 3.3.6"
echo ""

# Check file size of test.dat
if [ -f "../test.dat" ]; then
    size=$(stat -c%s "../test.dat")
    echo "Test file size: $size bytes ($(($size / 1024)) KB)"
    
    if [ $size -gt 100000000 ]; then
        echo "WARNING: Large file detected. Consider using increased heap size:"
        echo "   java -Xmx8g -jar target/scala-2.13/*.jar ../test.dat"
    fi
fi