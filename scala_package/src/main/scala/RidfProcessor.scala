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

// Import our existing RIDF decoder logic
import oedo.udfs.decoders.RIDFBlockDecoder

object RidfProcessor {
  
  // Constants
  private val WORD_SIZE = 4
  private val MAX_FILE_SIZE = 2L * 1024 * 1024 * 1024  // 2GB
  
  case class RidfBlock(blockId: Int, blockData: Array[Byte])
  
  // Structure for decoded event data
  case class EventData(
    run_number: Int,
    event_number: Int,
    timestamp: Long,
    fp: Int,
    dev: Int,
    det: Int,
    mod: Int,
    data: Array[Byte]
  )
  
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
  
  // Memory-optimized batch sizes
  private val BATCH_SIZE = 5000  // Reduced from 10000 to use less memory
  private val EVENT_BATCH_SIZE = 1000000  // Max events to accumulate before writing
  private val QUEUE_CAPACITY = 2  // Reduced queue size
  private val POOL_SIZE = 2
  
  // Special marker to indicate end of reading
  case class EndOfData()
  
  def processRidf(spark: SparkSession, ridfFileName: String, outputFileName: String, nBlock: Long = Long.MaxValue): Unit = {
    val inputStream = new BufferedInputStream(new FileInputStream(ridfFileName))
    
    // Create a blocking queue for batches
    val batchQueue: BlockingQueue[Either[ArrayBuffer[RidfBlock], EndOfData]] = new LinkedBlockingQueue(QUEUE_CAPACITY) // Buffer up to 3 batches
    
    // Create thread pool for async processing
    val executor = Executors.newFixedThreadPool(POOL_SIZE)
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(executor)
    
    // Extract run number from first block (block 0)
    var globalRunNumber = 0
    
    try {
      // Get file size for progress reporting
      val file = new java.io.File(ridfFileName)
      val fileSize = file.length()
      println(s"File opened: $ridfFileName")
      println(s"File size: $fileSize bytes")
      println(s"Starting parallel processing: reading and decoding simultaneously...")
      
      var blockCount = 0L
      var totalBytesRead = 0L
      
      // Start async processor thread
      val processorFuture = Future {
        var batchNumber = 0
        var continue = true
        while (continue) {
          batchQueue.take() match {
            case Left(blocks) =>
              println(f"[PROCESSOR] Processing batch $batchNumber with ${blocks.length} blocks...")
              val start = System.currentTimeMillis()
              
              // Extract run number from first batch if not set
              if (globalRunNumber == 0 && blocks.nonEmpty) {
                globalRunNumber = RIDFBlockDecoder.extractRunNumber(blocks.head.blockData)
                println(f"[PROCESSOR] Extracted run number: $globalRunNumber")
              }
              
              processAndWriteBatch(spark, outputFileName, blocks, batchNumber, globalRunNumber)
              val duration = System.currentTimeMillis() - start
              println(f"[PROCESSOR] Completed batch $batchNumber in ${duration}ms")
              batchNumber += 1
            case Right(_) =>
              println(s"[PROCESSOR] Finished processing. Total batches: $batchNumber")
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
        
        // Send this batch to processor if we have blocks
        if (blocks.nonEmpty) {
          val progress = totalBytesRead.toDouble / fileSize * 100.0
          println(f"[READER] Queuing batch with ${blocks.length} blocks for processing. Progress: block_count: $blockCount, ${totalBytesRead}bytes/${fileSize}bytes ($progress%.1f%%)")
          batchQueue.put(Left(blocks)) // This will block if queue is full, providing backpressure
        }
      }
      
      // Signal end of data to processor
      batchQueue.put(Right(EndOfData()))
      println("[READER] Finished reading, waiting for processor to complete...")
      
      // Wait for processor to finish
      processorFuture.onComplete {
        case Success(_) => println("All processing completed successfully!")
        case Failure(exception) => println(s"Processor failed: ${exception.getMessage}")
      }
      
      // Block until processor completes
      scala.concurrent.Await.result(processorFuture, scala.concurrent.duration.Duration.Inf)
      
    } finally {
      inputStream.close()
      executor.shutdown()
    }
  }
  
  def processAndWriteBatch(spark: SparkSession, baseOutputName: String, blocks: ArrayBuffer[RidfBlock], batchNumber: Int, runNumber: Int): Unit = {
    import spark.implicits._
    
    // Process blocks in smaller chunks to avoid OOM
    val eventBatches = ArrayBuffer[ArrayBuffer[EventData]]()
    var currentBatch = ArrayBuffer[EventData]()
    
    blocks.foreach { block =>
      try {
        // Decode this block using our existing logic
        val events = RIDFBlockDecoder.decodeRIDFBlock(block.blockData)
        
        events.foreach { eventRow =>
          val eventNumber = eventRow.getInt(0)
          val timestamp = eventRow.getLong(1)
          val segDataArray = eventRow.getAs[Array[org.apache.spark.sql.Row]](2)
          
          segDataArray.foreach { segRow =>
            val fp = segRow.getInt(0)
            val device = segRow.getInt(1)
            val detector = segRow.getInt(2)
            val module = segRow.getInt(3)
            val address = segRow.getInt(4)
            val size = segRow.getInt(5)
            val data = segRow.getAs[Array[Byte]](6)
            
            currentBatch += EventData(
              run_number = runNumber,
              event_number = eventNumber,
              timestamp = timestamp,
              fp = fp,
              dev = device,
              det = detector,
              mod = module,
              data = data
            )
            
            // Write batch when it gets too large
            if (currentBatch.length >= EVENT_BATCH_SIZE) {
              eventBatches += currentBatch
              currentBatch = ArrayBuffer[EventData]()
            }
          }
        }
      } catch {
        case e: Exception =>
          println(f"[PROCESSOR] Error decoding block ${block.blockId}: ${e.getMessage}")
      }
    }
    
    // Add remaining events
    if (currentBatch.nonEmpty) {
      eventBatches += currentBatch
    }
    
    // Process each event batch separately to avoid OOM
    var totalEvents = 0
    eventBatches.zipWithIndex.foreach { case (eventBatch, subBatchIndex) =>
      if (eventBatch.nonEmpty) {
        try {
          val eventsDF = spark.createDataFrame(eventBatch.toSeq)
          
          // For the very first sub-batch, overwrite; for all others, append
          val writeMode = if (batchNumber == 0 && subBatchIndex == 0) "overwrite" else "append"
          
          // Write processed events to Parquet
          eventsDF.coalesce(1)
            .write
            .mode(writeMode)
            .option("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
            .parquet(baseOutputName)
          
          totalEvents += eventBatch.length
          println(f"[PROCESSOR] Written sub-batch $batchNumber.$subBatchIndex with ${eventBatch.length} events (mode: $writeMode)")
        } catch {
          case e: Exception =>
            println(f"[PROCESSOR] Error writing sub-batch $batchNumber.$subBatchIndex: ${e.getMessage}")
        } finally {
          // Help GC by clearing the batch
          eventBatch.clear()
        }
      }
    }
    
    if (totalEvents > 0) {
      println(f"[PROCESSOR] Completed batch $batchNumber with total $totalEvents decoded events")
    } else {
      println(f"[PROCESSOR] No events decoded from batch $batchNumber")
    }
  }
  
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Usage: RidfProcessor [input_file] [output_file] [n_block(optional)]")
      sys.exit(1)
    }
    
    val inputFileName = args(0)
    val outputFileName = args(1)
    val nBlock = if (args.length >= 3) args(2).toLong else Long.MaxValue
    
    // Create Spark session with memory optimizations
    val spark = SparkSession.builder()
      .appName("RIDF Processor - Direct Event Extraction")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.execution.arrow.pyspark.enabled", "false")
      .config("spark.sql.execution.arrow.maxRecordsPerBatch", "1000")
      .config("spark.sql.inMemoryColumnarStorage.compressed", "true")
      .config("spark.sql.parquet.compression.codec", "snappy")
      .master("local[*]")  // Use all available cores locally
      .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")  // Reduce log verbosity
    
    try {
      println(s"Starting RIDF processing: $inputFileName -> $outputFileName")
      println("This will decode events and save structured data (not raw blocks)")
      processRidf(spark, inputFileName, outputFileName, nBlock)
      println("Processing completed successfully")
    } catch {
      case e: Exception =>
        println(s"Error during processing: ${e.getMessage}")
        e.printStackTrace()
        sys.exit(1)
    } finally {
      spark.stop()
    }
  }
}