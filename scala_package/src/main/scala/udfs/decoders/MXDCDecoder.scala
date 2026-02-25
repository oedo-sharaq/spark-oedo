package decoders
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.Row
import java.nio.ByteBuffer
import java.nio.ByteOrder

object MXDCDecoder {
  
  // Mesytec XDC header masks and shifts
  val kNChannels: Int = 32
  val kHeader: Int = 0x01
  val kMeasure: Int = 0x00
  val kEOB: Int = 0x11

  val kHeaderMask: Int = 0xC0000000
  val kGeoMask: Int = 0x00FF0000
  val kChMask: Int = 0x001F0000
  val kMeasurementMask: Int = 0x0000FFFF

  val kHeaderShift: Int = 30
  val kGeoShift: Int = 16
  val kChShift: Int = 16
  val kMeasurementShift: Int = 0
 
  // Scala UDF (TModuleDecoderMXDC::Decodeと同等)
  def decode(binaryData: Array[Byte]): Seq[Row] = {
    if (binaryData == null || binaryData.length < 4) {
      return Seq.empty
    }
    
    // Convert byte array to int array
    val buffer = ByteBuffer.wrap(binaryData).order(ByteOrder.LITTLE_ENDIAN)
    val evtsize = binaryData.length / 4
    val evtdata = new Array[Int](evtsize)
    
    for (i <- 0 until evtsize) {
      evtdata(i) = buffer.getInt()
    }
    
    var measurements = Array.empty[Row]
    var igeo = 0
    var evtFlag = false  // Event Flag
    
    for (i <- 0 until evtsize) {
      val ih = (evtdata(i) & kHeaderMask) >> kHeaderShift
      
      ih match {
        case `kHeader` =>
          evtFlag = true
          igeo = (evtdata(i) & kGeoMask) >> kGeoShift
          
        case `kMeasure` =>
          if (evtFlag) {
            val ich = (evtdata(i) & kChMask) >> kChShift
            val measure = (evtdata(i) & kMeasurementMask) >> kMeasurementShift
            
            measurements :+= Row(igeo, ich, measure)
          }
          else {
            // Break if no header
            return measurements.toSeq
          }
          
        case `kEOB` =>
          evtFlag = false

        case _ =>
          // Unknown header type
      }
    }
    
    measurements.toSeq
  }
  
  class DecodeMXDCSegData extends UDF1[Array[Byte], Seq[Row]] {
    override def call(binaryData: Array[Byte]): Seq[Row] = {
      decode(binaryData)
    }
  }

  // UDFの登録をする関数 (呼び出しはPySparkから)
  // return typeのスキーマを登録する。
  def registerUDF(spark: SparkSession): Unit = {
    val retType: DataType =
      ArrayType(StructType(Seq(
        StructField("geo", IntegerType,  nullable = false),
        StructField("channel", IntegerType,  nullable = false),
        StructField("measurement", IntegerType,  nullable = false)
      )))

    spark.udf.register("decode_mxdc_segdata", new DecodeMXDCSegData(), retType)
  }
}
