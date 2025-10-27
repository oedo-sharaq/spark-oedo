package decoders
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.Row
import java.nio.ByteBuffer
import java.nio.ByteOrder

object RFSOCDecoder {
  
  // Scala UDF
  def decode(binaryData: Array[Byte]): Array[Short] = {
    if (binaryData == null || binaryData.length < 4) {
      return Array.empty[Short]
    }
    
    // Convert byte array to int array
    val buffer = ByteBuffer.wrap(binaryData).order(ByteOrder.LITTLE_ENDIAN)
    val evtsize = binaryData.length / 4
    val evtdata = new Array[Int](evtsize)
    
    for (i <- 0 until evtsize) {
      evtdata(i) = buffer.getInt()
    }

    val adcdata = evtdata.map { value =>
      // Extract bits 0 to 11 (12 bits) for ADC value
      ((value & 0xFFFF).toShort >> 4).toShort
    }

    adcdata
  }

  class DecodeRFSOCSegData extends UDF1[Array[Byte], Array[Short]] {
    override def call(binaryData: Array[Byte]): Array[Short] = {
      decode(binaryData)
    }
  }

  // UDFの登録をする関数 (呼び出しはPySparkから)
  // return typeのスキーマを登録する。
  def registerUDF(spark: SparkSession): Unit = {
    val retType: DataType = ArrayType(ShortType)
    spark.udf.register("decode_rfsoc_segdata", new DecodeRFSOCSegData(), retType)
  }
}
