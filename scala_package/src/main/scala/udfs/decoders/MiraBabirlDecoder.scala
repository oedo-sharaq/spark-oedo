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
  def decode(binaryData: Array[Byte]): Array[Int] = {
    if (binaryData == null || binaryData.length < 2) {
      return Array.empty[Int]
    }
    
    // Convert byte array to short array
    val buffer = ByteBuffer.wrap(binaryData).order(ByteOrder.LITTLE_ENDIAN)
    val evtsize = binaryData.length / 2
    val evtdata = new Array[Int](evtsize)
    
    for (i <- 0 until evtsize) {
      var value = buffer.getShort()
      evtdata(i) = (value & 0xFFFF)
    }
    
    evtdata
  }

  def decodeTS(binaryData: Array[Byte]): Long = {
    if (binaryData == null || binaryData.length < 16) {
      return 0L
    }
    
    // Convert byte array to long array
    val buffer = ByteBuffer.wrap(binaryData).order(ByteOrder.LITTLE_ENDIAN)
    val tsHigh = buffer.getShort(12)
    val tsLow = buffer.getShort(14)

    // Treat the 16-bit parts as unsigned by masking with 0xFFFFL before combining.
    // This avoids sign-extension when a Short is negative.
    val tsvalue = ((tsHigh & 0xFFFFL) << 16) | (tsLow & 0xFFFFL)
    tsvalue
  }
  
  class DecodeMiraSegData extends UDF1[Array[Byte], Array[Int]] {
    override def call(binaryData: Array[Byte]): Array[Int] = {
      decode(binaryData)
    }
  }

  class DecodeMiraTSData extends UDF1[Array[Byte], Long] {
    override def call(binaryData: Array[Byte]): Long = {
      decodeTS(binaryData)
    }
  }
  // UDFの登録をする関数 (呼び出しはPySparkから)
  // return typeのスキーマを登録する。
  def registerUDF(spark: SparkSession): Unit = {
    val retType: DataType = ArrayType(IntegerType)

    spark.udf.register("decode_mira_segdata", new DecodeMiraSegData(), retType)
    spark.udf.register("decode_mira_tsdata", new DecodeMiraTSData(), LongType)
  }
}
