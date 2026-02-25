package decoders
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.Row
import java.nio.ByteBuffer
import java.nio.ByteOrder

object SIS3820Decoder {
  
  // Scala UDF (TModuleDecoderSIS3820::Decodeと同等)
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
    
    for (i <- 0 until evtsize) {
      measurements :+= Row(i, evtdata(i))
    }
    
    measurements.toSeq
  }
  
  class DecodeSIS3820SegData extends UDF1[Array[Byte], Seq[Row]] {
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
        StructField("measurement", IntegerType,  nullable = false)
      )))

    spark.udf.register("decode_sis3820_segdata", new DecodeSIS3820SegData(), retType)
  }
}
