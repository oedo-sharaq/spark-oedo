package decoders
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.Row
import java.nio.ByteBuffer
import java.nio.ByteOrder

object LUPOTSDecoder {
  
  // Scala UDF 
  def decode(binaryData: Array[Byte]): Long = {
    if (binaryData == null || binaryData.length < 2) {
      return 0L
    }
    
    // Convert byte array to int array
    val buffer = ByteBuffer.wrap(binaryData).order(ByteOrder.LITTLE_ENDIAN)
    val evtsize = binaryData.length / 2

    if (evtsize == 3) {
      val evtdata = new Array[Short](evtsize)
      
      for (i <- 0 until evtsize) {
        evtdata(i) = buffer.getShort()
      }
      val timestamp = (evtdata(0).toLong & 0xffL) + ((evtdata(1).toLong << 16) ) + ((evtdata(2).toLong << 32) )
      timestamp

    } else {
      0L
    }
  }

  class DecodeLUPOTSData extends UDF1[Array[Byte], Long] {
    override def call(binaryData: Array[Byte]): Long = {
      decode(binaryData)
    }
  }

  // UDFの登録をする関数 (呼び出しはPySparkから)
  // return typeのスキーマを登録する。
  def registerUDF(spark: SparkSession): Unit = {
    val retType: DataType = LongType
    spark.udf.register("decode_lupots_segdata", new DecodeLUPOTSData(), retType)
  }
}
