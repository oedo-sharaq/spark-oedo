import java.io._
import java.nio.{ByteBuffer, ByteOrder}
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
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.schema.Types
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName._
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop.example.ExampleParquetWriter

case class WaveformRecord(
  event_id: Long,
  timestamp: Long,
  rev: Int,
  dev: Int,
  fp: Int,
  det: Int,
  mod: Int,
  trace: Array[Short]
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
              val trace = new Array[Short](traceBytes.length / 2)
              for (j <- trace.indices) {
                trace(j) = traceBuffer.getShort()
              }
              
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
                  trace = trace
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
      .repeated(INT32).named("trace")
      .named("WaveformRecord")
    
    val conf = new Configuration()
    val path = new Path(filename)
    val factory = new SimpleGroupFactory(schema)
    
    val writer = ExampleParquetWriter.builder(path)
      .withWriteMode(org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE)
      .withRowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE)
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
    val inputFile = if (args.length > 0) args(0) else "test.dat"
    val outputFile = if (args.length > 1) args(1) else "test_output.parquet"
    
    val startTime = System.currentTimeMillis()
    
    println(s"Processing MIRA file: $inputFile")
    
    val waveforms = decodeMiraFile(inputFile)
    println(s"Found ${waveforms.length} waveform records")
    
    if (waveforms.nonEmpty) {
      println(s"Writing to Parquet: $outputFile")
      writeToParquet(waveforms, outputFile)
    } else {
      println("No waveform data found!")
    }
    
    val endTime = System.currentTimeMillis()
    println(s"Total processing time: ${endTime - startTime} ms")
  }
}