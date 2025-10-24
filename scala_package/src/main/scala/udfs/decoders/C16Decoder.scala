package decoders
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.Row
import java.nio.ByteBuffer
import java.nio.ByteOrder

object C16Decoder {
  
  // Scala UDF (TArtDecoderV1190::Decodeと同等)
  def decode(binaryData: Array[Byte]): Seq[Row] = {
    if (binaryData == null || binaryData.length < 2) {
      return Seq.empty
    }
    
    // Convert byte array to int array
    val buffer = ByteBuffer.wrap(binaryData).order(ByteOrder.LITTLE_ENDIAN)
    val evtsize = binaryData.length / 2
    val evtdata = new Array[Short](evtsize)
    
    for (i <- 0 until evtsize) {
      evtdata(i) = buffer.getShort()
    }
    
    var measurements = Array.empty[Row]
    
    for (i <- 0 until evtsize) {
            measurements :+= Row(i, evtdata(i))
    }
    measurements.toIndexedSeq
  }
  
  class DecodeC16SegData extends UDF1[Array[Byte], Seq[Row]] {
    override def call(binaryData: Array[Byte]): Seq[Row] = {
      decode(binaryData)
    }
  }

  // UDFの登録をする関数 (呼び出しはPySparkから)
  // return typeのスキーマを登録する。
  def registerUDF(spark: SparkSession): Unit = {
    val retType: DataType =
      ArrayType(StructType(Seq(
        StructField("channel", IntegerType,  nullable = false),
        StructField("measurement", ShortType,  nullable = false),
      )))

    spark.udf.register("decode_c16_segdata", new DecodeC16SegData(), retType)
  }
}
