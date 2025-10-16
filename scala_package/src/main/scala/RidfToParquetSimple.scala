import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import java.io.{FileInputStream, BufferedInputStream}
import java.nio.ByteBuffer
import java.nio.ByteOrder
import scala.collection.mutable.ArrayBuffer
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue, ExecutorService, Executors}
import scala.concurrent.{Future, ExecutionContext}
import scala.util.{Success, Failure}

object RidfToParquetSimple {
  
  // Constants
  private val WORD_SIZE = 4
  private val MAX_FILE_SIZE = 2L * 1024 * 1024 * 1024  // 2GB
  
  case class RidfBlock(blockId: Int, blockData: Array[Byte])
  
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
  
  def readRidfHeader(buffer: Array[Byte], offset: Int): (Int, Int, Int) = {
    if (offset + 8 > buffer.length) {  // RIDF header is 8 bytes
      return (0, 0, 0)
    }
    
    val headerBuffer = ByteBuffer.wrap(buffer, offset, 8).order(ByteOrder.LITTLE_ENDIAN)
    val headerLong = headerBuffer.getLong()
    
    // Extract bit fields according to RIDF header structure
    val blockSize = (headerLong & 0x3FFFFFL).toInt         // bits 0-21 (22 bits)
    val classId = ((headerLong >> 22) & 0x3FL).toInt       // bits 22-27 (6 bits)  
    val layer = ((headerLong >> 28) & 0x3L).toInt          // bits 28-29 (2 bits)
    
    (blockSize, classId, layer)
  }
  
  
  
  // Batch size for processing (number of blocks to accumulate before writing)
  private val BATCH_SIZE = 10000
  
  // Special marker to indicate end of reading
  case class EndOfData()
  
  def readRidf(spark: SparkSession, ridfFileName: String, outputFileName: String, nBlock: Long = Long.MaxValue): Unit = {
    val inputStream = new BufferedInputStream(new FileInputStream(ridfFileName))
    
    // Create a blocking queue for batches
    val batchQueue: BlockingQueue[Either[ArrayBuffer[RidfBlock], EndOfData]] = new LinkedBlockingQueue(3) // Buffer up to 3 batches
    
    // Create thread pool for async writing
    val executor = Executors.newFixedThreadPool(2)
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(executor)
    
    try {
      // Get file size for progress reporting
      val file = new java.io.File(ridfFileName)
      val fileSize = file.length()
      println(s"File opened: $ridfFileName")
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
      val buffer = new Array[Byte](8) // For header
      
      while (totalBytesRead < fileSize && blockCount < nBlock) {
        val blocks = ArrayBuffer[RidfBlock]()
        var batchBlockCount = 0
        
        // Process one batch of blocks
        var continueReading = true
        while (continueReading && batchBlockCount < BATCH_SIZE && totalBytesRead < fileSize && blockCount < nBlock) {
          // Read header (8 bytes)
          val headerBytesRead = inputStream.read(buffer, 0, 8)
          if (headerBytesRead != 8) {
            continueReading = false
          } else {
            val (blockSize, classId, layer) = readRidfHeader(buffer, 0)
            
            // Debug first few blocks
            if (blockCount < 5) {
              println(f"[READER] Block $blockCount: size=$blockSize, classId=$classId, layer=$layer")
            }
            
            // Validate block size
            if (blockSize <= 0 || blockSize > 1000000) {
              println(f"[READER] Invalid block size $blockSize at block $blockCount")
              continueReading = false
            } else {
              val blockDataSize = blockSize * 2 - 8 // Total block size minus header
              if (blockDataSize <= 0) {
                println(f"[READER] Invalid block data size $blockDataSize")
                continueReading = false
              } else {
                // Read the complete block (header + data)
                val completeBlockSize = blockSize * 2
                val completeBlock = new Array[Byte](completeBlockSize)
                
                // Copy header to complete block
                Array.copy(buffer, 0, completeBlock, 0, 8)
                
                // Read remaining data
                val remainingBytes = completeBlockSize - 8
                val dataBytesRead = inputStream.read(completeBlock, 8, remainingBytes)
                if (dataBytesRead != remainingBytes) {
                  println(f"[READER] Could not read complete block data: expected $remainingBytes, got $dataBytesRead")
                  continueReading = false
                } else {
                  // Add block to current batch
                  blocks += RidfBlock(blockCount.toInt, completeBlock)
                  blockCount += 1
                  batchBlockCount += 1
                  totalBytesRead += completeBlockSize
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
  
  def writeParquetBatch(spark: SparkSession, baseOutputName: String, blocks: ArrayBuffer[RidfBlock], batchNumber: Int): Unit = {
    import spark.implicits._
    
    // Convert blocks to DataFrame
    val df = spark.createDataFrame(blocks.map(block => (block.blockId, block.blockData)).toSeq).toDF("block_id", "block_data")
    
    // For the first batch, create the directory with overwrite
    // For subsequent batches, append to the same directory
    val writeMode = if (batchNumber == 0) "overwrite" else "append"
    
    // Write to the same Parquet directory for all batches
    df.coalesce(1)
      .write
      .mode(writeMode)
      .option("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
      .parquet(baseOutputName)
    
    println(f"Written batch $batchNumber with ${blocks.length} blocks to: $baseOutputName (mode: $writeMode)")
  }
  
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Usage: RidfToParquet [input_file] [output_file] [n_block(optional)]")
      sys.exit(1)
    }
    
    val inputFileName = args(0)
    val outputFileName = args(1)
    val nBlock = if (args.length >= 3) args(2).toLong else Long.MaxValue
    
    // Create Spark session
    val spark = SparkSession.builder()
      .appName("RIDF to Parquet Converter")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
      .master("local[*]")  // Use all available cores locally
      .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")  // Reduce log verbosity
    
    try {
      println(s"Starting RIDF conversion: $inputFileName -> $outputFileName")
      readRidf(spark, inputFileName, outputFileName, nBlock)
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