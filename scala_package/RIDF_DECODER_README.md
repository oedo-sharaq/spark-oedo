# RIDF Block Decoder UDFs

This package provides two Spark UDFs for processing RIDF (RIKEN Data Format) binary data:

## UDFs

### 1. `extract_run_number(block_data: Array[Byte]) -> Int`

Extracts the run number from RIDF block data. This is typically used on block 0 which contains the comment headers with run information.

**Usage:**
```sql
SELECT extract_run_number(block_data) as run_number 
FROM ridf_table 
WHERE block_id = 0
```

### 2. `decode_ridf_block(block_data: Array[Byte]) -> Array[Event]`

Decodes RIDF block data into an array of events. Each event contains:
- `event_number`: Integer event ID
- `timestamp`: Long timestamp value  
- `segdata`: Array of segment data structures

Each segment contains:
- `fFP`: Focal plane ID
- `fDevice`: Device ID
- `fDetector`: Detector ID
- `fModule`: Module ID
- `fAddress`: Address value
- `size`: Data size in bytes
- `data`: Binary segment data

**Usage:**
```sql
SELECT explode(decode_ridf_block(block_data)) as event
FROM ridf_table
```

## Complete Example

```scala
// Register UDFs
RIDFBlockDecoder.registerUDF(spark)

// Read RIDF parquet data
val df = spark.read.parquet("path/to/ridf.parquet")

// Extract run number from first block
val runNumber = df.filter(col("block_id") === 0)
  .select(callUDF("extract_run_number", col("block_data")))
  .first().getInt(0)

// Decode all events
val events = df.select(
  explode(callUDF("decode_ridf_block", col("block_data"))).as("event")
).select(
  lit(runNumber).as("run_number"),
  col("event.event_number"),
  col("event.timestamp"), 
  col("event.segdata")
)
```

## Why Two Separate UDFs?

The run number is stored in block 0 (comment header) but individual blocks are processed in parallel by different Spark executors. To avoid broadcasting the run number to all executors, we:

1. Extract the run number from block 0 in the driver
2. Decode events without run numbers in parallel 
3. Add the run number as a literal column to all events

This approach is more efficient for large datasets with many blocks.