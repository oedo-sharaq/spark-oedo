package waveform
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.api.java.UDF3
import org.apache.spark.sql.Row

object TrapezoidalFilter {
  
  // Scala UDF (trapezoidal filtering)
  def process(waveform: Seq[Double], rise: Int, gap: Int): Seq[Double] = {
    if (waveform == null || waveform.isEmpty) {
      new Array[Double](0).toIndexedSeq
    } else {
      val x = waveform.toArray
      val n = x.length
      val out = new Array[Double](n)

      var acc: Double = 0.0
      var i = 0

      while (i < n) {
        val a = x(i)
        val b = if (i - rise >= 0) x(i - rise) else 0.0f
        val c = if (i - rise - gap >= 0) x(i - rise - gap) else 0.0f
        val d = if (i - 2 * rise - gap >= 0) x(i - 2 * rise - gap) else 0.0f

        val delta: Double = a - b - c + d
        acc += delta
        out(i) = acc

        i += 1
      }

      out.toIndexedSeq
    }
    
  }
  
  class TrapezoidalFilter extends UDF3[Seq[Double], Int, Int, Seq[Double]] {
    override def call(waveform: Seq[Double], rise: Int, gap: Int): Seq[Double] = {
      process(waveform, rise, gap)
    }
  }

  // UDFの登録をする関数 (呼び出しはPySparkから)
  // return typeのスキーマを登録する。
  def registerUDF(spark: SparkSession): Unit = {
    val retType: DataType =
      ArrayType(DoubleType)

    spark.udf.register("trapezoidal_filter", new TrapezoidalFilter(), retType)
  }
}
