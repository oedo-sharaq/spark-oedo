# RIDF to Parquet Converter

This Scala application converts RIDF (RIKEN Data Format) files to Parquet format using Apache Spark. It's equivalent to the C++ `ridfblock2parquet` application.

## Features

- Reads RIDF binary files and extracts blocks
- Converts to Parquet format with schema: `(block_id: Int, block_data: Binary)`
- Supports file splitting when output size exceeds 2GB
- Progress reporting during conversion
- Memory-efficient processing using Spark

## Usage

### Using the shell script (recommended):
```bash
./ridf_to_parquet.sh <input_ridf_file> <output_parquet_file> [max_blocks]
```

Examples:
```bash
# Convert entire file
./ridf_to_parquet.sh /path/to/input.ridf /path/to/output.parquet

# Convert only first 10000 blocks
./ridf_to_parquet.sh /path/to/input.ridf /path/to/output.parquet 10000
```

### Using spark-submit directly:
```bash
spark-submit --class RidfToParquet \
             --master local[*] \
             --driver-memory 4g \
             target/scala-2.12/spark-oedo-package_2.12-1.0.jar \
             input.ridf \
             output.parquet \
             [max_blocks]
```

## Building

```bash
sbt package
```

## Output Format

The output Parquet files contain two columns:
- `block_id`: Integer identifier for each RIDF block (0, 1, 2, ...)  
- `block_data`: Binary data of the RIDF block (raw bytes)

If the output exceeds 2GB, multiple files will be created with sequential numbering:
- `output.parquet` (or `output_000.parquet`)
- `output_001.parquet`  
- `output_002.parquet`
- etc.

## RIDF Block Structure

Each RIDF block header contains:
- Block size (22 bits): Size in 16-bit words
- Class ID (6 bits): Block type identifier
- Layer (2 bits): Layer information  
- Reserved (2 bits)
- Address (32 bits): Additional addressing information

## Requirements

- Apache Spark 3.5.0+
- Scala 2.12
- Java 8 or 11

## Performance Notes

- The application loads the entire input file into memory for easier processing
- For very large files (>available RAM), consider modifying to use streaming I/O
- Spark runs locally by default (`local[*]`) - modify for cluster deployment
- Memory settings can be adjusted in the shell script based on file sizes