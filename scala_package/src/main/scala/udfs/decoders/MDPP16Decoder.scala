package decoders
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.Row
import java.nio.ByteBuffer
import java.nio.ByteOrder

object MDPP16Decoder {

  // MDPP16 TDC header masks and shifts
  val kHeader: Int = 0x00000001
  val kEvent: Int = 0x00000000
  val kEnder: Int = 0x18000011
  
  val kSubMask: Int = 0xc0000000
  val kSubMaskShift: Int = 30
  val kGeoMask: Int = 0x00ff0000
  val kGeoShift: Int = 16
  val kTempSizeMask: Int = 0x000003ff
  val kTempSizeShift: Int = 0
  val kChMask: Int = 0x001f0000
  val kChShift: Int = 16
  val kDataMask: Int = 0x0000ffff
  val kDataShift: Int = 0

  // Scala UDF (TArtDecoderMDPP16::Decodeと同等)
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
    var igeo = -1
    var ich = 0
    
    for (i <- 0 until evtsize) {
      val msk = (evtdata(i) & kSubMask) >> kSubMaskShift
      
      msk match {
        case `kHeader` =>
          igeo = (evtdata(i) & kGeoMask) >> kGeoShift
          var tmpsize = (evtdata(i) & kTempSizeMask) >> kTempSizeShift
          if (tmpsize + 1 != evtsize) {
            // Size mismatch, break decoding
            println(s"MDPP16Decoder: Size mismatch detected. Expected ${tmpsize + 1}, got $evtsize")
            return measurements.toIndexedSeq
          }
          
        case `kEvent` =>
          if (igeo != -1) {
            // Event
            ich = (evtdata(i) & kChMask) >> kChShift
            val measure = (evtdata(i) & kDataMask) >> kDataShift
            measurements :+= Row(igeo, ich, measure)
          }
          
        case `kEnder` =>
          // Ender
          igeo = -1
          
        case _ =>
          // Unknown header type
      }
    }

    measurements.toIndexedSeq
  }
  
  class DecodeMDPP16SegData extends UDF1[Array[Byte], Seq[Row]] {
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
      )))

    spark.udf.register("decode_mdpp16_segdata", new DecodeMDPP16SegData(), retType)
  }
}
