import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import java.io.{FileInputStream, BufferedInputStream}
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.charset.StandardCharsets
import scala.collection.mutable.ArrayBuffer
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue, ExecutorService, Executors}
import scala.concurrent.{Future, ExecutionContext}
import scala.util.{Success, Failure}

object StreamingV1ToParquet {
  
  // Constants
  private val WORD_SIZE = 4
  private val MAX_FILE_SIZE = 2L * 1024 * 1024 * 1024  // 2GB
  
  case class TimeFrameBlock(timeFrameId: Int, numSource: Int, timeFrameType: Int, blockData: Array[Byte])
  
  def generateFileName(baseName: String, sequence: Int): String = {
    if (sequence == 0) {
      baseName
    } else {
      val dotPos = baseName.lastIndexOf('.')
      if (dotPos == -1) {
        // No extension
        f"${baseName}_$sequence%03d"
      } else {
        // Has extension
        val name = baseName.substring(0, dotPos)
        val ext = baseName.substring(dotPos)
        f"${name}_$sequence%03d$ext"
      }
    }
  }
  
  def readFileSinkHeader(buffer: Array[Byte], offset: Int): (Int, Int, Int) = {

    val headerBuffer = ByteBuffer.wrap(buffer, offset, 0x130).order(ByteOrder.LITTLE_ENDIAN)

    val nameBytes = new Array[Byte](8)
    headerBuffer.get(nameBytes)
    val nameStr = new String(nameBytes, StandardCharsets.UTF_8)
    println(s"TimeFrame Block Name: $nameStr")

    val length = headerBuffer.getInt()
    val headerSize = headerBuffer.getShort() & 0xFFFF
    val headerType = headerBuffer.getShort() & 0xFFFF
    val fairMQDeviceType = headerBuffer.getLong()
    val runNumber = headerBuffer.getLong()
    val startUnixTime = headerBuffer.getLong()
    val stopUnixTime = headerBuffer.getLong()

    val commentsBytes = new Array[Byte](256)
    headerBuffer.get(commentsBytes)
    val commentStr = new String(commentsBytes, StandardCharsets.UTF_8)

    println(s"FairMQ Device Type: $fairMQDeviceType")
    println(s"Run Number: $runNumber")
    println(s"Start Unix Time: $startUnixTime")
    println(s"Stop Unix Time: $stopUnixTime")
    println(s"Comments: $commentStr")

    (headerSize.toInt, runNumber.toInt, headerType.toInt)
  }

  def readTimeFrameHeader(buffer: Array[Byte], offset: Int): (Int, Int, Int, Int, Int) = {
    
    val headerBuffer = ByteBuffer.wrap(buffer, offset, 24).order(ByteOrder.LITTLE_ENDIAN)

    val nameBytes = new Array[Byte](8)
    headerBuffer.get(nameBytes)
    val nameStr = new String(nameBytes, StandardCharsets.UTF_8)
    //println(s"TimeFrame Block Name: $nameStr")

    val blockSize = headerBuffer.getInt()
    val headerSize = headerBuffer.getShort() & 0xFFFF
    val blockType = headerBuffer.getShort() & 0xFFFF
    val numSource = headerBuffer.getInt()
    val timeFrameId = headerBuffer.getInt()

    //println(s"TimeFrame Header: blockSize=$blockSize, headerSize=$headerSize, blockType=$blockType, numSource=$numSource, timeFrameId=$timeFrameId")
    
    (blockSize, headerSize, blockType, timeFrameId, numSource)
  } 
  
  
  // Batch size for processing (number of blocks to accumulate before writing)
  private val BATCH_SIZE = 100000
  private val FILESINK_HEADER_SIZE = 0x130
  private val TIMEFRAME_HEADER_SIZE = 24
  
  // Special marker to indicate end of reading
  case class EndOfData()
  
  def readFile(spark: SparkSession, datFileName: String, outputFileName: String, nBlock: Long = Long.MaxValue): Unit = {
    val inputStream = new BufferedInputStream(new FileInputStream(datFileName))
    
    // Create a blocking queue for batches
    val batchQueue: BlockingQueue[Either[ArrayBuffer[TimeFrameBlock], EndOfData]] = new LinkedBlockingQueue(3) // Buffer up to 3 batches
    
    // Create thread pool for async writing
    val executor = Executors.newFixedThreadPool(2)
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(executor)
    
    try {
      // Get file size for progress reporting
      val file = new java.io.File(datFileName)
      val fileSize = file.length()
      println(s"File opened: $datFileName")
      println(s"File size: $fileSize bytes")
      println(s"Starting parallel processing: reading and writing simultaneously...")
      
      var blockCount = 0L
      var totalBytesRead = 0L
      
      // Start async writer thread
      val writerFuture = Future {
        var batchNumber = 0
        var continue = true
        while (continue) {
          batchQueue.take() match {
            case Left(blocks) =>
              println(f"[WRITER] Writing batch $batchNumber with ${blocks.length} blocks...")
              val start = System.currentTimeMillis()
              writeParquetBatch(spark, outputFileName, blocks, batchNumber)
              val duration = System.currentTimeMillis() - start
              println(f"[WRITER] Completed batch $batchNumber in ${duration}ms")
              batchNumber += 1
            case Right(_) =>
              println(s"[WRITER] Finished writing. Total batches: $batchNumber")
              continue = false
          }
        }
      }
      
      // Reader thread (main thread)
      println("[READER] Starting to read blocks...")
      val buffer = new Array[Byte](FILESINK_HEADER_SIZE) // For header
      val headerBytesRead = inputStream.read(buffer, 0, FILESINK_HEADER_SIZE)
      if (headerBytesRead != FILESINK_HEADER_SIZE) {
        throw new Exception(s"Could not read complete FileSink header: expected ${FILESINK_HEADER_SIZE} bytes, got $headerBytesRead")
      }
      val (headerSize, runNumber, headerType) = readFileSinkHeader(buffer, 0)
      println(s"FileSink Header Size: $headerSize, Run Number: $runNumber, Header Type: $headerType")
      totalBytesRead += headerBytesRead

      var continueReading = true
      while (continueReading && totalBytesRead < fileSize && blockCount < nBlock) {
        val blocks = ArrayBuffer[TimeFrameBlock]()
        var batchBlockCount = 0
        
        // Process one batch of blocks
        while (continueReading && batchBlockCount < BATCH_SIZE && totalBytesRead < fileSize && blockCount < nBlock) {
          // Read header (24 bytes)
          val headerBytesRead = inputStream.read(buffer, 0, TIMEFRAME_HEADER_SIZE)
          if (headerBytesRead != TIMEFRAME_HEADER_SIZE) {
            continueReading = false
          } else {
            val (blockSize, headerSize, blockType, timeFrameId, numSource) = readTimeFrameHeader(buffer, 0)
            totalBytesRead += headerBytesRead
            
            // Debug first few blocks
            if (blockCount < 5) {
              println(f"[READER] Block $blockCount: size=$blockSize, headerSize=$headerSize, blockType=$blockType, timeFrameId=$timeFrameId, numSource=$numSource")
            }
            //println(f"blockCount: $blockCount, blockSize: $blockSize, totalBytesRead: $totalBytesRead") 
            // Validate block size
            if (blockSize <= 0 || blockSize > 1000000) {
              println(f"[READER] Invalid block size $blockSize at block $blockCount")
              continueReading = false
            } else {
              val blockDataSize = blockSize - headerSize // Total block size minus header
              if (blockDataSize <= 0) {
                println(f"[READER] Invalid block data size $blockDataSize")
                continueReading = false
              } else {
                val dataBlock = new Array[Byte](blockDataSize)
                
                // Read remaining data
                val dataBytesRead = inputStream.read(dataBlock, 0, blockDataSize)
                if (dataBytesRead != blockDataSize) {
                  println(f"[READER] Could not read block data: expected $blockDataSize, got $dataBytesRead")
                  continueReading = false
                } else {
                  // Add block to current batch
                  blocks += TimeFrameBlock(timeFrameId, numSource, blockType, dataBlock)
                  blockCount += 1
                  batchBlockCount += 1
                  totalBytesRead += blockDataSize
                }
              }
            }
          }
        }
        
        // Send this batch to writer if we have blocks
        if (blocks.nonEmpty) {
          val progress = totalBytesRead.toDouble / fileSize * 100.0
          println(f"[READER] Queuing batch with ${blocks.length} blocks for writing. Progress: block_count: $blockCount, ${totalBytesRead}bytes/${fileSize}bytes ($progress%.1f%%)")
          batchQueue.put(Left(blocks)) // This will block if queue is full, providing backpressure
        }
      }
      
      // Signal end of data to writer
      batchQueue.put(Right(EndOfData()))
      println("[READER] Finished reading, waiting for writer to complete...")
      
      // Wait for writer to finish
      writerFuture.onComplete {
        case Success(_) => println("All processing completed successfully!")
        case Failure(exception) => println(s"Writer failed: ${exception.getMessage}")
      }
      
      // Block until writer completes
      scala.concurrent.Await.result(writerFuture, scala.concurrent.duration.Duration.Inf)
      
    } finally {
      inputStream.close()
      executor.shutdown()
    }
  }
  
  def writeParquetBatch(spark: SparkSession, baseOutputName: String, blocks: ArrayBuffer[TimeFrameBlock], batchNumber: Int): Unit = {
    import spark.implicits._

    // Convert blocks to DataFrame
    val df = spark.createDataFrame(blocks.map(block => (block.timeFrameId, block.numSource, block.timeFrameType, block.blockData)).toSeq).toDF("time_frame_id", "num_source", "type", "data")
    val decoded_df = df.withColumn("decoded_events", expr("decode_tf_block(data)"))
      .select(
        col("time_frame_id"),
        col("num_source"),
        col("type"),
        explode(col("decoded_events")).alias("event")
      )
      .select(
        col("time_frame_id"),
        col("num_source"),
        col("type"),
        col("event.version").alias("version"),
        col("event.magic").alias("magic"),
        col("event.tfId").alias("tfId"),
        col("event.femType").alias("femType"),
        col("event.femId").alias("femId"),
        col("event.numMessages").alias("numMessages"),
        col("event.timeSec").alias("timeSec"),
        col("event.timeUSec").alias("timeUSec"),
        col("event.subMagic").alias("subMagic"),
        col("event.data").alias("data")
      )

    // For the first batch, create the directory with overwrite
    // For subsequent batches, append to the same directory
    val writeMode = if (batchNumber == 0) "overwrite" else "append"
    
    // Write to the same Parquet directory for all batches
    decoded_df.coalesce(1)
      .write
      .mode(writeMode)
      .option("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
      .parquet(baseOutputName)
    
    println(f"Written batch $batchNumber with ${blocks.length} blocks to: $baseOutputName (mode: $writeMode)")
  }
  
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Usage: StreamingV1ToParquet [input_file] [output_file] [n_block(optional)]")
      sys.exit(1)
    }
    
    val inputFileName = args(0)
    val outputFileName = args(1)
    val nBlock = if (args.length >= 3) args(2).toLong else Long.MaxValue
    
    // Create Spark session
    val spark = SparkSession.builder()
      .appName("StreamingV1 to Parquet Converter")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
      .master("local[*]")  // Use all available cores locally
      .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")  // Reduce log verbosity
    oedo.udfs.decoders.StreamingV1TFDecoder.registerUDF(spark)
    
    try {
      println(s"Starting StreamingV1 conversion: $inputFileName -> $outputFileName")
      readFile(spark, inputFileName, outputFileName, nBlock)
      println("Conversion completed successfully")
    } catch {
      case e: Exception =>
        println(s"Error during conversion: ${e.getMessage}")
        e.printStackTrace()
        sys.exit(1)
    } finally {
      spark.stop()
    }
  }
}