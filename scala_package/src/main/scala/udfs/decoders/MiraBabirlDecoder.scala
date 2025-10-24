package decoders
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.Row
import java.nio.ByteBuffer
import java.nio.ByteOrder

object MiraBabirlDecoder {
  
  // Scala UDF 
  def decode(binaryData: Array[Byte]): Array[Short] = {
    if (binaryData == null || binaryData.length < 2) {
      return Array.empty[Short]
    }
    
    // Convert byte array to short array
    val buffer = ByteBuffer.wrap(binaryData).order(ByteOrder.LITTLE_ENDIAN)
    val evtsize = binaryData.length / 2
    val evtdata = new Array[Short](evtsize)
    
    for (i <- 0 until evtsize) {
      evtdata(i) = buffer.getShort()
    }
    
    evtdata
  }
  
  class DecodeMiraSegData extends UDF1[Array[Byte], Array[Short]] {
    override def call(binaryData: Array[Byte]): Array[Short] = {
      decode(binaryData)
    }
  }

  // UDFの登録をする関数 (呼び出しはPySparkから)
  // return typeのスキーマを登録する。
  def registerUDF(spark: SparkSession): Unit = {
    val retType: DataType = ArrayType(ShortType)

    spark.udf.register("decode_mira_segdata", new DecodeMiraSegData(), retType)
  }
}
