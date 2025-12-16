package oedo.udfs.decoders

import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.Row
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.charset.StandardCharsets
import scala.collection.mutable.ArrayBuffer

object StreamingV1TFDecoder {
  
  // Header structure
  case class HeaderLengthData(
    version: Int,
    magic: String,
    headerLength: Int,
    length: Int,
    headerType: Int
  )

  case class SubTimeFrameData(
    tfId: Int,
    femType: Int,
    femId: Int,
    numMessages: Int,
    timeSec: Long,
    timeUSec: Long
  )

  case class EventResult(
    version: Int,
    magic: String,
    tfId: Int,
    femType: Int,
    femId: Int,
    numMessages: Int,
    timeSec: Long,
    timeUSec: Long,
    subMagic: String,
    data: Array[Byte]
  )
  
  private val HEADER_LENGTH_SIZE = 16 // bytes
  // Parse header from 16 bytes (128 bits)
  def parseHeaderLength(bytes: Array[Byte], offset: Int): HeaderLengthData = {
    val headerBuffer = ByteBuffer.wrap(bytes, offset, HEADER_LENGTH_SIZE).order(ByteOrder.LITTLE_ENDIAN)

    // Use the offset into the provided byte array when reading the version
    val version: Int = bytes(offset) & 0xFF
    val nameBytes = new Array[Byte](8)
    headerBuffer.get(nameBytes)
    val nameStr = new String(nameBytes, StandardCharsets.UTF_8)

    // Extract bit fields according to C++ union structure
    val length = headerBuffer.getInt()
    val headerLength = headerBuffer.getShort() & 0xFFFF
    val headerType = headerBuffer.getShort() & 0xFFFF

    HeaderLengthData(version, nameStr, headerLength, length, headerType)
  }

  private val SUBTIME_HEADER_LENGTH_SIZE = 32 // bytes
  def parseSubTimeFrameHeader(bytes: Array[Byte], offset: Int): SubTimeFrameData = {
    val headerBuffer = ByteBuffer.wrap(bytes, offset, SUBTIME_HEADER_LENGTH_SIZE).order(ByteOrder.LITTLE_ENDIAN)
    val tfId = headerBuffer.getInt()
    val femType = headerBuffer.getInt()
    val femId = headerBuffer.getInt()
    val numMessages = headerBuffer.getInt()
    val timeSec = headerBuffer.getLong()
    val timeUSec = headerBuffer.getLong()

    SubTimeFrameData(tfId, femType, femId, numMessages, timeSec, timeUSec)
  }
  
  // decode binary data in a TimeFrame
  def decodeTimeFrame(bytes: Array[Byte]): Array[Row] = {
    if(bytes == null || bytes.length < HEADER_LENGTH_SIZE) {
      return Array.empty[Row]
    }

    var offset = 0
    var next = 0
    val blockSize = bytes.length
    var eob = false
    val events = ArrayBuffer[EventResult]()

    var percentComplete: Int = 0
    while(!eob) {
        val header = parseHeaderLength(bytes, next)
        offset = next
        next = offset + header.length

        if (header.magic.startsWith("SUBTIME")) {
          val subTime = parseSubTimeFrameHeader(bytes, offset + HEADER_LENGTH_SIZE)
          val subHeader = parseHeaderLength(bytes, offset + header.headerLength)
          //println(s"Decoded SubTimeFrame Header: tfId=${subTime.tfId}, femType=${subTime.femType}, femId=${subTime.femId}, numMessages=${subTime.numMessages}, timeSec=${subTime.timeSec}, timeUSec=${subTime.timeUSec}, subMagic=${subHeader.magic}, subLength=${subHeader.length}, subHeaderLength=${subHeader.headerLength}")
          var event = EventResult(
            version = header.version,
            magic = header.magic,
            tfId = subTime.tfId,
            femType = subTime.femType,
            femId = subTime.femId,
            numMessages = subTime.numMessages,
            timeSec = subTime.timeSec,
            timeUSec = subTime.timeUSec,
            subMagic = subHeader.magic,
            data = Array.empty[Byte]
          )
          if (subHeader.magic.startsWith("HRTBEAT")) {
            val hbDataLength = subHeader.length - subHeader.headerLength
            val hbOffset = offset + header.headerLength + subHeader.headerLength
            if (hbDataLength > 0 && hbOffset >= 0 && hbOffset + hbDataLength <= bytes.length) {
              val hbData = new Array[Byte](hbDataLength)
              System.arraycopy(bytes, hbOffset, hbData, 0, hbDataLength)
              event = event.copy(data = hbData)
            } else {
              println(s"[WARN] Skipping HRTBEAT: bounds invalid - offset=$hbOffset, length=$hbDataLength, bytesLen=${bytes.length}")
            }
          }
          events += event
        }
        else{
          val unknownDataLength = header.length - header.headerLength
          val unknownDataOffset = offset + header.headerLength
          if (unknownDataLength > 0 && unknownDataOffset >= 0 && unknownDataOffset + unknownDataLength <= bytes.length) {
            val unknownData = new Array[Byte](unknownDataLength)
            System.arraycopy(bytes, unknownDataOffset, unknownData, 0, unknownDataLength)
            events += EventResult(
              version = header.version,
              magic = header.magic,
              tfId = -1,
              femType = -1,
              femId = -1,
              numMessages = -1,
              timeSec = -1,
              timeUSec = -1,
              subMagic = "",
              data = unknownData
            )
          } else {
            println(s"[WARN] Skipping unknown block: bounds invalid - offset=$unknownDataOffset, length=$unknownDataLength, bytesLen=${bytes.length}")
          }
        }
        if(blockSize <= next) {
          eob = true
          //println(s"Reached end of block. Current offset: $next, Block size: $blockSize")
        }
    }
    // Convert events to Rows for DataFrame
    val result = events.map { event =>
      Row(event.version,
          event.magic,
          event.tfId,
          event.femType,
          event.femId,
          event.numMessages,
          event.timeSec,
          event.timeUSec,
          event.subMagic,
          event.data)
    }.toArray
    result
  }
  
  class DecodeStreamingV1TFData extends UDF1[Array[Byte], Array[Row]] {
    override def call(binaryData: Array[Byte]): Array[Row] = {
      decodeTimeFrame(binaryData)
    }
  }
 
  // Spark SQL function wrapper for PySpark
  import org.apache.spark.sql.Column
  import org.apache.spark.sql.functions.udf

  // Register UDFs with Spark session
  def registerUDF(spark: SparkSession): Unit = {
    val eventDataType = StructType(Seq(
      StructField("version", IntegerType, nullable = false),
      StructField("magic", StringType, nullable = false),
      StructField("tfId", IntegerType, nullable = false),
      StructField("femType", IntegerType, nullable = false),
      StructField("femId", IntegerType, nullable = false),
      StructField("numMessages", IntegerType, nullable = false),
      StructField("timeSec", LongType, nullable = false),
      StructField("timeUSec", LongType, nullable = false),
      StructField("subMagic", StringType, nullable = false),
      StructField("data", BinaryType, nullable = false)
    ))

    val eventRetType: DataType = ArrayType(eventDataType)

    // Register event decoder UDF (without run number)
    spark.udf.register("decode_tf_block", new DecodeStreamingV1TFData(), eventRetType)
    
  }
  
  // For direct import in PySpark - these functions should be accessible from Python
  object functions {
    def decode_tf_block(column: Column): Column = {
      val decodeUDF = udf((data: Array[Byte]) => decodeTimeFrame(data))
      decodeUDF(column)  
    }//
  }
}