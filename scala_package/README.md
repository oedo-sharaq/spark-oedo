# Spark-OEDO Scala Package

This Scala package provides comprehensive data processing tools for the OEDO experimental system, including RIDF (RIKEN Ion-beam Data Format) processing and MIRA raw data decoding.

## Overview

The package contains two main components:

### 1. RIDF Block Decoder UDF
For processing RIDF block data in Apache Spark, extracting:
- `run_number`: Run number from comment headers
- `event_number`: Event number for each event
- `timestamp`: Event timestamp from headers  
- `segments`: Array of segment data structures

### 2. MIRA Decoder
Complete Scala translation of the C++ MIRA raw data decoder with:
- **RIDF Format Parsing**: Full support for MIRA's RIDF binary format
- **Apache Arrow Output**: Returns Arrow tables instead of C++ vectors
- **Parquet Writing**: Built-in Parquet file writing with compression
- **Event Decoding**: Faithful translation of decode_buffer() and decode_an_event()
- **Waveform Analysis**: QDC/ADC calculations and channel processing

## Features

### RIDF Processing
- Binary block data decoding
- Segment extraction with metadata
- Spark UDF integration for distributed processing

### MIRA Decoder Features
- **Immutable Data Structures**: Scala case classes for type safety
- **Timestamp Extraction**: 64-bit timestamp reconstruction from RIDF headers
- **Channel Data Processing**: Multi-channel waveform support
- **Arrow Table Output**: Native Arrow table creation with proper schema
- **Parquet Support**: Compressed output with Snappy compression

## Quick Start

### Prerequisites
- Scala 2.13+
- Apache Spark 4.0.0
- SBT (Scala Build Tool)
- Java 8+

### Building
```bash
cd scala_package
sbt compile
```

### MIRA Decoder Usage
```bash
# Basic usage
sbt "runMain MiraDecoderMain input_data.dat"

# Specify output file and channels
sbt "runMain MiraDecoderMain input_data.dat output.parquet 0 1 2"
```

### RIDF Processing Usage
```bash
# Run RIDF to Parquet converter
sbt "runMain RidfToParquetSimple input.ridf output.parquet"
```

## Usage

1. **Register the UDF in your Spark session:**
```scala
import oedo.udfs.decoders.RIDFBlockDecoder

val spark = SparkSession.builder()
  .appName("Your App")
  .getOrCreate()

RIDFBlockDecoder.registerUDF(spark)
```

2. **Use in DataFrame operations:**
```scala
// Assuming you have a DataFrame with binary block data
val df = spark.read.format("binaryFile").load("path/to/ridf/files")

// Decode RIDF blocks
val decodedDF = df.select(
  col("path"),
  explode(expr("decode_ridf_block(content)")).as("event")
).select(
  col("path"),
  col("event.run_number"),
  col("event.event_number"),
  col("event.timestamp"),
  col("event.segments")
)

decodedDF.show()
```

3. **Use in Spark SQL:**
```sql
SELECT 
  path,
  event.run_number,
  event.event_number,
  event.timestamp,
  event.segments
FROM (
  SELECT 
    path,
    explode(decode_ridf_block(content)) as event
  FROM ridf_files_table
)
```

4. **Access individual segments:**
```scala
val segmentDF = decodedDF.select(
  col("path"),
  col("run_number"), 
  col("event_number"),
  explode(col("segments")).as("segment")
).select(
  col("path"),
  col("run_number"),
  col("event_number"),
  col("segment.fFP"),
  col("segment.fDevice"),
  col("segment.fDetector"),
  col("segment.fModule"),
  col("segment.fAddress"),
  col("segment.size"),
  col("segment.data")
)
```

## Data Format

The UDF handles the following RIDF class IDs:
- Class ID 3: Event header without timestamp
- Class ID 4: Segment data  
- Class ID 5: Comment header (contains run number)
- Class ID 6: Event header with timestamp
- Class ID 8: Block counter (skipped)
- Class ID 9: Block ender (skipped)
- Class ID 21: Status data (skipped)

## Building

```bash
cd scala_package
sbt compile
sbt package
```

## Implementation Details

The decoder is based on the C++ TArtParserRIDF implementation and handles:
- RIDF header parsing with size, class ID, and address extraction
- Segment ID parsing to extract FP, device, detector, and module information
- Binary data extraction for each segment
- Multiple events per block processing
- Proper byte ordering (little-endian)

The UDF returns an array of event structures, allowing multiple events per input block to be processed efficiently in Spark's distributed environment.