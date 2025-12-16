package decoders
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.Row
import java.nio.ByteBuffer
import java.nio.ByteOrder
import scala.annotation.switch

object HRTDCDecoder {

  case class DecodedMeasurement(
    data: Seq[TDCData],
    hbf: HBFData
  )

  case class TDCData(
    ch: Int,
    time: Int,
    tot: Int
  ) 

  case class HBFData(
    flag: Int,
    userReg: Int,
    timeOffset: Int,
    hbfNumber: Int,
    generatedDataSize: Int,
    transferedDataSize: Int
  )

  val kShiftHeader: Int = 58;
  val kHeaderMask: Int = 0x3f;  // header mask
  val kHeaderTDC: Int = 0x0b;   // tdc data
  val kHeaderTDCT: Int = 0x0d;  // (b001101) tdc trailing data
  val kHeaderTDCT1S: Int = 0x19;  // (b011001) input throttling t1 start
  val kHeaderTDCT1E: Int = 0x11;  // (b010001) input throttling t1 end
  val kHeaderTDCT2S: Int = 0x1a;  // (b011010) input throttling t2 start
  val kHeaderTDCT2E: Int = 0x12;  // (b010010) input throttling t2 end
  val kHeaderHBD: Int = 0x1c;  // heartbeat delimiter

  val kShiftChannel: Int = 51;
  val kMaskChannel: Int = 0x7f;
  val kShiftTOT: Int = 29;
  val kMaskTOT: Int = 0x3fffff;
  val kShiftTime: Int = 0;
  val kMaskTime: Int = 0x1fffffff;

  val kDelim1: Int = 0x1c
  val kDelim2: Int = 0x1e

  // Scala UDF
  def decode(binaryData: Array[Byte]): DecodedMeasurement = {
    if (binaryData == null || binaryData.length < 4) {
      return DecodedMeasurement(Seq.empty[TDCData], HBFData(0,0,0,0,0,0))
    }
    
    // Convert byte array to int array
    val buffer = ByteBuffer.wrap(binaryData).order(ByteOrder.LITTLE_ENDIAN)
    val evtsize = binaryData.length / 8

    var measurements = Seq.empty[TDCData] 
    var flag = 0
    var userReg = 0
    var timeOffset = 0
    var hbfNumber = 0
    var generatedDataSize = 0
    var transferedDataSize = 0

    for (i <- 0 until evtsize) {
      val evtdata = buffer.getLong()
      val ih = (evtdata >>> kShiftHeader).toInt & kHeaderMask

      ih match {
        case `kHeaderTDC` | `kHeaderTDCT` | `kHeaderTDCT1S` | `kHeaderTDCT1E` | `kHeaderTDCT2S` | `kHeaderTDCT2E` =>
          val ch = (evtdata >>> kShiftChannel).toInt & kMaskChannel
          val tot = (evtdata >>> kShiftTOT).toInt & kMaskTOT
          val time = (evtdata >>> kShiftTime).toInt & kMaskTime
          measurements :+= TDCData(ch, time, tot)
        case `kHeaderHBD` =>
          val delim = (evtdata >>> 58).toInt & 0x3f
          flag = (evtdata >>> 40).toInt & 0xffff
          if (delim == kDelim1) {
            timeOffset = (evtdata >>> 24).toInt & 0xffff
            hbfNumber = (evtdata & 0xffffff).toInt
          } else if (delim == kDelim2) {
            userReg = (evtdata >>> 40).toInt & 0xffff
            generatedDataSize = (evtdata >>> 20).toInt & 0xfffff
            transferedDataSize = (evtdata & 0xfffff).toInt
          }

        case _ =>

      }
    }
    val hbfData = HBFData(
      flag,
      userReg,
      timeOffset,
      hbfNumber,
      generatedDataSize,
      transferedDataSize
    )
    // Return the decoded measurement
    DecodedMeasurement(measurements, hbfData)
  }
  
  class DecodeHRTDCData extends UDF1[Array[Byte], DecodedMeasurement] {
    override def call(binaryData: Array[Byte]): DecodedMeasurement = {
      decode(binaryData)
    }
  }

  // UDFの登録をする関数 (呼び出しはPySparkから)
  // return typeのスキーマを登録する。
  def registerUDF(spark: SparkSession): Unit = {
    spark.udf.register("decode_hrtdc_segdata", new DecodeHRTDCData(),
      StructType(Seq(
        StructField("data", ArrayType(
          StructType(Seq(
            StructField("ch", IntegerType, nullable = false),
            StructField("time", IntegerType, nullable = false),
            StructField("tot", IntegerType, nullable = false)
          ))
        ), nullable = false),
        StructField("hbf", 
          StructType(Seq(
            StructField("flag", IntegerType, nullable = false),
            StructField("userReg", IntegerType, nullable = false),
            StructField("timeOffset", IntegerType, nullable = false),
            StructField("hbfNumber", IntegerType, nullable = false),
            StructField("generatedDataSize", IntegerType, nullable = false),
            StructField("transferedDataSize", IntegerType, nullable = false)
          )), 
        nullable = false)
      ))
    )
  }
}
