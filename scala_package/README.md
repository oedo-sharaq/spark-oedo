# RIDF Block Decoder UDF

This Scala UDF decodes RIDF (RIKEN Ion-beam Data Format) block data for use in Apache Spark.

## Overview

The RIDF Block Decoder UDF takes binary block data as input and outputs decoded event information including:
- `run_number`: The run number extracted from comment headers
- `event_number`: Event number for each event
- `timestamp`: Event timestamp (for events with timestamp headers)  
- `segments`: Array of segment data structures

Each segment contains:
- `fFP`: FP (Focal Plane) identifier
- `fDevice`: Device identifier
- `fDetector`: Detector identifier  
- `fModule`: Module identifier
- `fAddress`: Memory address
- `size`: Data size in bytes
- `data`: Binary segment data

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