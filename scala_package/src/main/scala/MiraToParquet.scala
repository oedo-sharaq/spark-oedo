import java.io._
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.file.{Files, Paths}
import java.util.concurrent.Executors
import scala.collection.mutable.{ListBuffer, ArrayBuffer}
import scala.util.control.Breaks._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.schema.Types
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName._
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import scala.concurrent._
import scala.concurrent.duration._

case class WaveformRecord(
  event_id: Long,
  timestamp: Long,
  rev: Int,
  dev: Int,
  fp: Int,
  det: Int,
  mod: Int,
  traceIdx: Int,
  traceGeo: Int,
  traceTS: Long,
  trace: Array[Int]
)

object MiraToParquet {
  
  def decodeMiraFile(filename: String): Array[WaveformRecord] = {
    val results = ArrayBuffer[WaveformRecord]()
    
    try {
      val file = new File(filename)
      val fis = new FileInputStream(file)
      
      try {
        var i = 0 // event number
        var currentTimestamp = 0L
        
        breakable {
        while (true) {
          // Read 8 bytes (2 x 4-byte integers)
          val bytes = new Array[Byte](8)
          val bytesRead = fis.read(bytes)
          if (bytesRead != 8) {
            break()
          }
          
          val buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
          val w1 = buffer.getInt()
          val w2 = buffer.getInt()
          
          val rev = (w1 >>> 30) & 0x3
          val ly = (w1 >>> 28) & 0x3
          val cid = (w1 >>> 22) & 0x3f
          val size = w1 & 0x3fffff // self inclusive
          val addr = w2
          
          if (cid == 6) { // event with timestamp
            // Read 12 more bytes (3 x 4-byte integers)
            val eventBytes = new Array[Byte](12)
            val eventBytesRead = fis.read(eventBytes)
            if (eventBytesRead != 12) {
              break()
            }
            
            val eventBuffer = ByteBuffer.wrap(eventBytes).order(ByteOrder.LITTLE_ENDIAN)
            val evtn = eventBuffer.getInt()
            val tsLow = eventBuffer.getInt()
            val tsHigh = eventBuffer.getInt()
            currentTimestamp = (tsHigh.toLong << 32) | (tsLow.toLong & 0xffffffffL)
            
            i += 1
            // Progress tracking removed for cleaner output
          }
          
          if (cid == 4) { // segment
            val bufSize = 2 * size - 8 // Total bytes minus the 8 bytes we already read
            val buf = new Array[Byte](bufSize)
            val bufBytesRead = fis.read(buf)
            if (bufBytesRead != bufSize) {
              break()
            }
            
            // Extract segid from first 4 bytes
            val segidBuffer = ByteBuffer.wrap(buf, 0, 4).order(ByteOrder.LITTLE_ENDIAN)
            val segid = segidBuffer.getInt()
            
            val segRev = (segid >>> 26) & 0x3f
            val segDev = (segid >>> 20) & 0x3f
            val segFp = (segid >>> 14) & 0x3f
            val segDet = (segid >>> 8) & 0x3f
            val segMod = segid & 0xff
            
            if (segDet < 14) { // det should be interpreted as channel number; but det >= 12 are virtual
              // Extract trace data (skip first 4 bytes which are segid)
              val traceBytes = buf.slice(4, buf.length)
              val traceBuffer = ByteBuffer.wrap(traceBytes).order(ByteOrder.LITTLE_ENDIAN)
              val trace = new Array[Int](traceBytes.length / 2)
              for (j <- trace.indices) {
                val t = traceBuffer.getShort()
                trace(j) = if (t > 0) t & 0xffff else t + 65536  // unsigned short
              }
              val traceIdx = trace(0)
              val traceGeo = trace(2)
              val traceTsHi = if (trace(6) > 0) trace(6) else trace(6) + 65536 // unsigned short
              val traceTsLo = if (trace(7) > 0) trace(7) else trace(7) + 65536 // unsigned short
              val traceTS = (traceTsHi.toLong << 16) | (traceTsLo.toLong & 0xffffL)
              
              // first event is always bad because of firmware issue
              if (i != 1) {
                results += WaveformRecord(
                  event_id = i.toLong,
                  timestamp = currentTimestamp,
                  rev = segRev,
                  dev = segDev,
                  fp = segFp,
                  det = segDet,
                  mod = segMod,
                  traceIdx = traceIdx,
                  traceGeo = traceGeo,
                  traceTS = traceTS,
                  trace = trace.slice(8, trace.length-2) // skip first 8 shorts which are metadata (trace[0], trace[2], trace[6], trace[7] are used for metadata; the rest are waveform data
                )
              }
            }
          }
        }
        }
        
      } finally {
        fis.close()
      }
      
    } catch {
      case e: Exception =>
        println(s"Error during decoding: ${e.getMessage}")
        e.printStackTrace()
    }
    
    results.toArray
  }

  def writeToParquet(data: Array[WaveformRecord], filename: String): Unit = {
    // Define Parquet schema
    val schema = Types.buildMessage()
      .required(INT64).named("event_id")
      .required(INT64).named("timestamp")
      .required(INT32).named("rev")
      .required(INT32).named("dev")
      .required(INT32).named("fp")
      .required(INT32).named("det")
      .required(INT32).named("mod")
      .required(INT32).named("traceIdx")
      .required(INT32).named("traceGeo")
      .required(INT64).named("traceTS")
      .repeated(INT32).named("trace")
      .named("WaveformRecord")
    
    val conf = new Configuration()
    val path = new Path(filename)
    val factory = new SimpleGroupFactory(schema)
    
    val writer = ExampleParquetWriter.builder(path)
      .withWriteMode(org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE)
      .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
      .withCompressionCodec(CompressionCodecName.SNAPPY)
      .withConf(conf)
      .withType(schema)
      .build()
    
    try {
      for (record <- data) {
        val group = factory.newGroup()
        group.add("event_id", record.event_id)
        group.add("timestamp", record.timestamp)
        group.add("rev", record.rev)
        group.add("dev", record.dev)
        group.add("fp", record.fp)
        group.add("det", record.det)
        group.add("mod", record.mod)
        group.add("traceIdx", record.traceIdx)
        group.add("traceGeo", record.traceGeo)
        group.add("traceTS", record.traceTS)

        // Add trace array - each value as a separate entry in the repeated field
        for (value <- record.trace) {
          group.add("trace", value.toInt)
        }
        
        writer.write(group)
      }
      
      println(s"Successfully wrote ${data.length} records to Parquet file: $filename")
      
    } finally {
      writer.close()
    }
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Usage: MiraToParquet <input_file.dat> <output_dir> [chunk_size]")
      System.exit(1)
    }
    
    val inputFile = args(0)
    val outputDir = args(1)
    val chunkSize = if (args.length > 2) args(2).toInt else 50000
    val numThreads = 8
    
    // Create output directory if it doesn't exist
    val outputPath = Paths.get(outputDir)
    if (!Files.exists(outputPath)) {
      Files.createDirectories(outputPath)
    }
    
    // Create a fixed thread pool with 8 threads
    val executor = Executors.newFixedThreadPool(numThreads)
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(executor)
    
    val startTime = System.currentTimeMillis()
    
    println(s"Processing MIRA file: $inputFile")
    
    val waveforms = decodeMiraFile(inputFile)
    println(s"Found ${waveforms.length} waveform records")
    
    if (waveforms.nonEmpty) {
      // Split into chunks of fixed size
      val chunks = waveforms.grouped(chunkSize).toArray
      
      val futures = chunks.zipWithIndex.map { case (chunk, index) =>
        Future {
          val outputFile = s"$outputDir/part_$index.parquet"
          println(s"Writing ${chunk.length} records to Parquet: $outputFile")
          writeToParquet(chunk, outputFile)
        }
      }.toSeq
      
      // Wait for all futures to complete
      Await.result(Future.sequence(futures), Duration.Inf)
      
      println(s"Successfully wrote ${waveforms.length} records to ${chunks.length} Parquet files in $outputDir")
    } else {
      println("No waveform data found!")
    }
    
    val endTime = System.currentTimeMillis()
    println(s"Total processing time: ${endTime - startTime} ms")
    
    // Shutdown the executor
    executor.shutdown()
  }
}
