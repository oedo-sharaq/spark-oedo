package decoders
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.Row
import java.nio.ByteBuffer
import java.nio.ByteOrder

object V1290Decoder {
  
  // V1290 TDC header masks and shifts
  val kHeaderMask: Int = 0xF8000000
  val kGlobalHeader: Int = 0x40000000
  val kTDCHeader: Int = 0x08000000
  val kTDCMeasurement: Int = 0x00000000
  val kTDCTrailer: Int = 0x18000000
  val kTDCError: Int = 0x20000000
  val kGlobalTrailer: Int = 0x80000000
  
  val kMaskGeometry: Int = 0x0000001F
  val kShiftGeometry: Int = 0
  val kMaskChannel: Int = 0x03e00000
  val kShiftChannel: Int = 21
  val kMaskEdgeType: Int = 0x04000000
  val kShiftEdgeType: Int = 26
  val kMaskMeasure: Int = 0x0007FFFF
  val kShiftMeasure: Int = 0
  
  // Scala UDF (TArtDecoderV1290::Decodeと同等)
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
    var ghf = 0  // Global Header Flag
    
    for (i <- 0 until evtsize) {
      val ih = evtdata(i) & kHeaderMask
      
      ih match {
        case `kGlobalHeader` =>
          ghf = 1
          igeo = (evtdata(i) & kMaskGeometry) >> kShiftGeometry
          
        case `kTDCHeader` =>
          if (ghf != 1) {
            // Break if no global header
            return measurements.toSeq
          }
          
        case `kTDCMeasurement` =>
          if (ghf == 1) {
            val ich = (evtdata(i) & kMaskChannel) >> kShiftChannel
            val edge = (evtdata(i) & kMaskEdgeType) >> kShiftEdgeType
            val measure = (evtdata(i) & kMaskMeasure) >> kShiftMeasure
            
            measurements :+= Row(igeo, ich, measure, if (edge == 1) 1 else 0)
          }
          
        case `kTDCTrailer` =>
          // TDC Trailer - no action needed in this implementation
          
        case `kTDCError` =>
          // TDC Error - could handle errors here
          
        case `kGlobalTrailer` =>
          ghf = 0
          
        case _ =>
          // Unknown header type
      }
    }
    
    measurements.toSeq
  }
  
  class DecodeV1290SegData extends UDF1[Array[Byte], Seq[Row]] {
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
        StructField("measurement", IntegerType,  nullable = false),
        StructField("edge", IntegerType,  nullable = false)
      )))

    spark.udf.register("decode_v1290_segdata", new DecodeV1290SegData(), retType)
  }
}
