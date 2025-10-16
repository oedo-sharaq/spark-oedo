package oedo.udfs.decoders

import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.Row
import java.nio.ByteBuffer
import java.nio.ByteOrder
import scala.collection.mutable.ArrayBuffer

object RIDFBlockDecoder {
  
  // RIDF Header structure (matches C++ ArtHeader_t union)
  case class RIDFHeader(
    size: Int,      // 22 bits
    classID: Int,   // 6 bits 
    layer: Int,     // 2 bits
    reserved: Int,  // 2 bits
    address: Long   // 32 bits (unsigned)
  )
  
  // Segment ID structure (matches C++ ArtSegIDRIDF_t union)
  case class SegmentID(
    module: Int,    // 8 bits
    detector: Int,  // 6 bits
    fp: Int,        // 6 bits
    device: Int,    // 6 bits
    version: Int    // 6 bits
  )
  
  // Segment data structure for output
  case class SegmentData(
    fFP: Int,
    fDevice: Int,
    fDetector: Int,
    fModule: Int,
    fAddress: Int,
    size: Int,
    data: Array[Byte]
  )
  
  // Event result structure (without run number)
  case class EventResult(
    eventNumber: Int,
    timestamp: Long,
    segments: Array[SegmentData]
  )

  // Parse RIDF header from 8 bytes (64 bits) - matches C++ union exactly
  def parseRIDFHeader(bytes: Array[Byte], offset: Int): RIDFHeader = {
    val buffer = ByteBuffer.wrap(bytes, offset, 8).order(ByteOrder.LITTLE_ENDIAN)
    val headerLong = buffer.getLong()
    
    // Extract bit fields according to C++ union structure
    val size = (headerLong & 0x3FFFFFL).toInt         // bits 0-21 (22 bits)
    val classID = ((headerLong >> 22) & 0x3FL).toInt  // bits 22-27 (6 bits)
    val layer = ((headerLong >> 28) & 0x3L).toInt     // bits 28-29 (2 bits)
    val reserved = ((headerLong >> 30) & 0x3L).toInt  // bits 30-31 (2 bits)
    val address = (headerLong >>> 32) & 0xFFFFFFFFL   // bits 32-63 (32 bits, unsigned)
    
    RIDFHeader(size, classID, layer, reserved, address)
  }
  
  // Parse segment ID from 4 bytes (32 bits) - matches C++ union exactly
  def parseSegmentID(bytes: Array[Byte], offset: Int): SegmentID = {
    val buffer = ByteBuffer.wrap(bytes, offset, 4).order(ByteOrder.LITTLE_ENDIAN)
    val segidInt = buffer.getInt()
    
    val module = (segidInt & 0xFF)                    // bits 0-7 (8 bits)
    val detector = ((segidInt >> 8) & 0x3F)          // bits 8-13 (6 bits)
    val fp = ((segidInt >> 14) & 0x3F)               // bits 14-19 (6 bits)
    val device = ((segidInt >> 20) & 0x3F)           // bits 20-25 (6 bits)
    val version = ((segidInt >> 26) & 0x3F)          // bits 26-31 (6 bits)
    
    SegmentID(module, detector, fp, device, version)
  }
  
  // Helper functions for extracting primitives
  def getInt(bytes: Array[Byte], offset: Int): Int = {
    ByteBuffer.wrap(bytes, offset, 4).order(ByteOrder.LITTLE_ENDIAN).getInt()
  }
  
  def getLong(bytes: Array[Byte], offset: Int): Long = {
    ByteBuffer.wrap(bytes, offset, 8).order(ByteOrder.LITTLE_ENDIAN).getLong()
  }
  
  // Extract null-terminated string from bytes
  def getString(bytes: Array[Byte], offset: Int, maxLength: Int): String = {
    val endPos = math.min(offset + maxLength, bytes.length)
    val strBytes = bytes.slice(offset, endPos)
    val nullPos = strBytes.indexOf(0)
    val actualBytes = if (nullPos >= 0) strBytes.take(nullPos) else strBytes
    new String(actualBytes, "UTF-8").trim
  }

  // Extract run number from RIDF block (matches C++ TArtParserRIDF)
  def extractRunNumber(blockData: Array[Byte]): Int = {
    if (blockData == null || blockData.length < 16) {
      return 0
    }
    
    // Check if this is a block header (ClassID 1) and skip it
    var startOffset = 0
    if (blockData.length >= 8) {
      val firstHeader = parseRIDFHeader(blockData, 0)
      if (firstHeader.classID == 1) {
        startOffset = 8
      }
    }
    
    var offset = startOffset
    val blockSize = blockData.length
    
    // Look for ClassID 5 (comment header) - matches C++ implementation
    while (offset + 8 <= blockSize) {
      val header = parseRIDFHeader(blockData, offset)
      
      if (header.size == 0) {
        return -1
      }
      
      if (header.classID == 5) { // Comment header
        // Match C++ implementation exactly:
        // idx = fOffset + sizeof(int) * 3; // skip header
        val cidOffset = offset + 12  // Skip 3 integers (header + 2 more ints)
        
        if (cidOffset + 4 <= blockSize) {
          val cid = getInt(blockData, cidOffset)
          if (cid == 1) { // Comment ID 1 = Header
            // idx = fOffset + sizeof(int) * 4; // skip header  
            val infoOffset = offset + 16  // Skip 4 integers (16 bytes total)
            
            // Read ridf_comment_runinfost struct
            // The struct contains a runnumber field as a null-terminated string
            if (infoOffset < blockSize) {
              // Read the runnumber string from the struct (assuming it's at the beginning)
              val runNameStr = getString(blockData, infoOffset, math.min(100, blockSize - infoOffset))
              val runNumberStr = getString(blockData, infoOffset + 100, math.min(100, blockSize - infoOffset - 100))

              if (runNumberStr.nonEmpty) {
                try {
                  val runNumber = runNumberStr.toInt  // matches strtol(info.runnumber, NULL, 10)
                  return runNumber
                } catch {
                  case _: NumberFormatException =>
                    // If direct conversion fails, try to extract first numeric part
                    val numericPart = runNumberStr.takeWhile(c => c.isDigit || c == '-' || c == '+')
                    if (numericPart.nonEmpty) {
                      try {
                        val runNumber = numericPart.toInt
                        return runNumber
                      } catch {
                        case _: NumberFormatException => // Continue searching
                      }
                    }
                }
              }
            }
          }
        }
      }
      
      // Move to next header
      val nextOffset = offset + header.size * 2
      if (nextOffset <= offset || nextOffset >= blockSize) {
        return -1
      }
      offset = nextOffset
    }
    
    -1
  }

  def decodeRIDFBlock(blockData: Array[Byte]): Array[Row] = {
    if (blockData == null || blockData.length < 16) {
      return Array.empty[Row]
    }
    
    // Check if this is a block header (ClassID 1) and skip it
    var startOffset = 0
    if (blockData.length >= 8) {
      val firstHeader = parseRIDFHeader(blockData, 0)
      
      if (firstHeader.classID == 1) {
        // This is a block header, skip it and process inner content
        startOffset = 8
      }
    }
    
    val events = ArrayBuffer[EventResult]()
    var offset = startOffset
    var next = startOffset
    val blockSize = blockData.length
    var eob = false
    
    // Main parsing loop - follows C++ GetNextEvent logic exactly
    while (!eob) {
      // Check if we have enough data for header
      if (next + 8 > blockSize) {
        eob = true
      } else {
        // Read header at current position
        val header = parseRIDFHeader(blockData, next)
        offset = next
        val prev = next
        next = offset + header.size * 2  // size is in 16-bit words (sizeof(short))
        
        if (blockSize <= next) {
          eob = true
        }
        
        // Handle different class IDs - skip unsupported ones
        val supportedClassIDs = Set(0, 3, 4, 5, 6, 8, 9, 11, 12, 13, 21)
        if (!supportedClassIDs.contains(header.classID)) {
          // Skip unsupported class ID, but don't break - continue parsing
        } else {
          header.classID match {
            case 5 => // Comment header - skip in event decoder
              
            case 8 | 9 | 21 => // Block counter, block ender, status data - skip
              
            case 3 | 6 => // Event headers - process event data
              val segments = ArrayBuffer[SegmentData]()
              
              var eventOffset = offset + 8 // skip header (8 bytes)
              
              // Read event number
              if (eventOffset + 4 <= blockSize) {
                val eventNumber = getInt(blockData, eventOffset)
                eventOffset += 4
                
                var timestamp = 0L
                if (header.classID == 6 && eventOffset + 8 <= blockSize) {
                  // Read timestamp for class 6 (event with timestamp)
                  timestamp = getLong(blockData, eventOffset)
                  eventOffset += 8
                }
                
                // Process segments - scan until next event boundary
                while (eventOffset < next && eventOffset + 8 <= blockSize) {
                  val segHeader = parseRIDFHeader(blockData, eventOffset)
                  
                  if (segHeader.classID == 4) { // Segment data
                    val segIDOffset = eventOffset + 8
                    if (segIDOffset + 4 <= blockSize) {
                      val segID = parseSegmentID(blockData, segIDOffset)
                      
                      val dataSize = segHeader.size * 2 - 8 - 4 // total - header - segID
                      val dataOffset = segIDOffset + 4
                      
                      if (dataOffset + dataSize <= blockSize && dataSize > 0) {
                        val segmentData = blockData.slice(dataOffset, dataOffset + dataSize)
                        
                        val segData = SegmentData(
                          fFP = segID.fp,
                          fDevice = segID.device,
                          fDetector = segID.detector,
                          fModule = segID.module,
                          fAddress = segHeader.address.toInt,
                          size = dataSize,
                          data = segmentData
                        )
                        segments += segData
                      }
                    }
                  }
                  // Move to next segment
                  eventOffset += segHeader.size * 2
                }
                
                // Create event result
                val event = EventResult(eventNumber, timestamp, segments.toArray)
                events += event
              }
              
            case _ => // Will handle other cases
          }
        }
      }
    }
    
    // Convert events to Rows for DataFrame
    val result = events.map { event =>
      val segDataRows = event.segments.map { seg =>
        Row(seg.fFP, seg.fDevice, seg.fDetector, seg.fModule, seg.fAddress, seg.size, seg.data)
      }
      Row(event.eventNumber, event.timestamp, segDataRows)
    }.toArray
    result
  }
  
  class DecodeRIDFBlockData extends UDF1[Array[Byte], Array[Row]] {
    override def call(binaryData: Array[Byte]): Array[Row] = {
      decodeRIDFBlock(binaryData)
    }
  }

  class ExtractRunNumber extends UDF1[Array[Byte], Int] {
    override def call(binaryData: Array[Byte]): Int = {
      extractRunNumber(binaryData)
    }
  }
  
  // Spark SQL function wrapper for PySpark
  import org.apache.spark.sql.Column
  import org.apache.spark.sql.functions.udf
  
  // Function that returns a Column for use in PySpark
  def extract_run_number(column: Column): Column = {
    val extractUDF = udf((data: Array[Byte]) => extractRunNumber(data))
    extractUDF(column)
  }

  // Schema definition for event decoder (without run number)
  val eventOutputSchema = StructType(Seq(
    StructField("events", ArrayType(StructType(Seq(
      StructField("event_number", IntegerType, nullable = false),
      StructField("timestamp", LongType, nullable = false),
      StructField("segdata", ArrayType(StructType(Seq(
        StructField("fFP", IntegerType, nullable = false),
        StructField("fDevice", IntegerType, nullable = false), 
        StructField("fDetector", IntegerType, nullable = false),
        StructField("fModule", IntegerType, nullable = false),
        StructField("fAddress", IntegerType, nullable = false),
        StructField("size", IntegerType, nullable = false),
        StructField("data", BinaryType, nullable = false)
      ))), nullable = false)
    ))), nullable = false)
  ))

  // Register UDFs with Spark session
  def registerUDF(spark: SparkSession): Unit = {
    val segmentDataType = StructType(Seq(
      StructField("fFP", IntegerType, nullable = false),
      StructField("fDevice", IntegerType, nullable = false), 
      StructField("fDetector", IntegerType, nullable = false),
      StructField("fModule", IntegerType, nullable = false),
      StructField("fAddress", IntegerType, nullable = false),
      StructField("size", IntegerType, nullable = false),
      StructField("data", BinaryType, nullable = false)
    ))
    
    val eventDataType = StructType(Seq(
      StructField("event_number", IntegerType, nullable = false),
      StructField("timestamp", LongType, nullable = false),
      StructField("segdata", ArrayType(segmentDataType), nullable = false)
    ))

    val eventRetType: DataType = ArrayType(eventDataType)

    // Register event decoder UDF (without run number)
    spark.udf.register("decode_ridf_block", new DecodeRIDFBlockData(), eventRetType)
    
    // Register run number extractor UDF (for SQL usage)
    spark.udf.register("extract_run_number_sql", new ExtractRunNumber(), IntegerType)
  }
  
  // For direct import in PySpark - these functions should be accessible from Python
  object functions {
    def extract_run_number(column: Column): Column = {
      val extractUDF = udf((data: Array[Byte]) => extractRunNumber(data))
      extractUDF(column)
    }
    
    def decode_ridf_block(column: Column): Column = {
      val decodeUDF = udf((data: Array[Byte]) => decodeRIDFBlock(data))
      decodeUDF(column)  
    }//
  }
}